using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Server;
using Raven.Client.Server.Versioning;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Utils;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Versioning
{
    public unsafe class VersioningStorage
    {
        private static readonly Slice ChangeVectorSlice;
        private static readonly Slice IdAndEtagSlice;
        public static readonly Slice RevisionsEtagsSlice;
        private static readonly Slice RevisionDocumentsSlice;
        private static readonly Slice RevisionsCountSlice;

        private static readonly TableSchema DocsSchema;

        private readonly Logger _logger;
        private readonly DocumentDatabase _database;
        private readonly DocumentsStorage _documentsStorage;
        private readonly VersioningConfiguration _versioningConfiguration;

        private enum Columns
        {
            /* ChangeVector is the table's key as it's unique and will avoid conflicts (by replication) */
            ChangeVector = 0,
            LowerId = 1,
            /* We are you using the record separator in order to avoid loading another documents that has the same ID prefix, 
                e.g. fitz(record-separator)01234567 and fitz0(record-separator)01234567, without the record separator we would have to load also fitz0 and filter it. */
            RecordSeparator = 2,
            Etag = 3, // etag to keep the insertion order
            Id = 4,
            Document = 5,
            Flags = 6,
            LastModified = 7,
        }
        private readonly VersioningConfigurationCollection _emptyConfiguration = new VersioningConfigurationCollection();

        private VersioningStorage(DocumentDatabase database, VersioningConfiguration versioningConfiguration)
        {
            _database = database;
            _documentsStorage = _database.DocumentsStorage;
            _versioningConfiguration = versioningConfiguration;

            _logger = LoggingSource.Instance.GetLogger<VersioningStorage>(database.Name);

            using (var tx = database.DocumentsStorage.Environment.WriteTransaction())
            {
                DocsSchema.Create(tx, RevisionDocumentsSlice, 16);

                tx.CreateTree(RevisionsCountSlice);

                tx.Commit();
            }
        }

        static VersioningStorage()
        {
            Slice.From(StorageEnvironment.LabelsContext, "ChangeVector", ByteStringType.Immutable, out ChangeVectorSlice);
            Slice.From(StorageEnvironment.LabelsContext, "IdAndEtag", ByteStringType.Immutable, out IdAndEtagSlice);
            Slice.From(StorageEnvironment.LabelsContext, "RevisionsEtags", ByteStringType.Immutable, out RevisionsEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "RevisionDocuments", ByteStringType.Immutable, out RevisionDocumentsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "RevisionsCount", ByteStringType.Immutable, out RevisionsCountSlice);

            DocsSchema = new TableSchema();
            DocsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)Columns.ChangeVector,
                Count = 1,
                Name = ChangeVectorSlice
            });
            DocsSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)Columns.LowerId,
                Count = 3,
                Name = IdAndEtagSlice
            });
            DocsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)Columns.Etag,
                Name = RevisionsEtagsSlice
            });
        }

        public static VersioningStorage LoadConfigurations(DocumentDatabase database, DatabaseRecord dbRecord, VersioningStorage versioningStorage)
        {
            var logger = LoggingSource.Instance.GetLogger<VersioningStorage>(database.Name);
            try
            {
                if (dbRecord.Versioning == null)
                    return null;
                if (dbRecord.Versioning.Equals(versioningStorage?._versioningConfiguration))
                    return versioningStorage;                    
                var config = new VersioningStorage(database, dbRecord.Versioning);
                if (logger.IsInfoEnabled)
                    logger.Info("Versioning configuration changed");
                return config;
            }
            catch (Exception e)
            {
                //TODO: This should generate an alert, so admin will know that something is very bad
                //TODO: Or this should throw and we should have a config flag to ignore the error
                if (logger.IsOperationsEnabled)
                    logger.Operations(
                        $"Cannot enable versioning for documents as the versioning configuration" +
                        $" in the database record is missing or not valid: {dbRecord}", e);
                return null;
            }
        }

        private VersioningConfigurationCollection GetVersioningConfiguration(CollectionName collectionName)
        {
            VersioningConfigurationCollection configuration;
            if (_versioningConfiguration.Collections != null && _versioningConfiguration.Collections.TryGetValue(collectionName.Name, out configuration))
            {
                return configuration;
            }

            if (_versioningConfiguration.Default != null)
            {
                return _versioningConfiguration.Default;
            }

            return _emptyConfiguration;
        }

        public bool ShouldVersionDocument(CollectionName collectionName, NonPersistentDocumentFlags nonPersistentFlags, 
            BlittableJsonReaderObject existingDocument, BlittableJsonReaderObject document, ref DocumentFlags documentFlags, 
            out VersioningConfigurationCollection configuration)
        {
            configuration = GetVersioningConfiguration(collectionName);
            if (configuration.Active == false)
                return false;

            try
            {
                if ((nonPersistentFlags & NonPersistentDocumentFlags.FromSmuggler) != NonPersistentDocumentFlags.FromSmuggler)
                    return true;
                if (existingDocument == null)
                {
                    // we are not going to create a revision if it's an import from v3
                    // (since this import is going to import revisions as well)
                    return (nonPersistentFlags & NonPersistentDocumentFlags.LegacyVersioned) != NonPersistentDocumentFlags.LegacyVersioned;
                }

                // compare the contents of the existing and the new document
                if (DocumentCompare.IsEqualTo(existingDocument, document, false) != DocumentCompareResult.NotEqual)
                {
                    // no need to create a new revision, both documents have identical content
                    return false;
                }

                return true;
            }
            finally
            {
                documentFlags |= DocumentFlags.Versioned;
            }
        }

        public void Put(DocumentsOperationContext context, string id, BlittableJsonReaderObject document, 
            DocumentFlags flags, NonPersistentDocumentFlags nonPersistentFlags, ChangeVectorEntry[] changeVector, long lastModifiedTicks,
            VersioningConfigurationCollection configuration = null)
        {
            Debug.Assert(changeVector != null, "Change vector must be set");

            BlittableJsonReaderObject.AssertNoModifications(document, id, assertChildren: true);

            DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, id, out Slice lowerId, out Slice idPtr);

            var notFromSmuggler = (nonPersistentFlags & NonPersistentDocumentFlags.FromSmuggler) != NonPersistentDocumentFlags.FromSmuggler;

            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, RevisionDocumentsSlice);
            fixed (ChangeVectorEntry* pChangeVector = changeVector)
            {
                var changeVectorPtr = (byte*)pChangeVector;
                var changeVectorSize = sizeof(ChangeVectorEntry) * changeVector.Length;

                // We want the revision's attachments to have a lower etag than the revision itself
                if ((flags & DocumentFlags.HasAttachments) == DocumentFlags.HasAttachments &&
                    notFromSmuggler)
                {
                    using (Slice.External(context.Allocator, changeVectorPtr, changeVectorSize, out Slice changeVectorSlice))
                    {
                        if (table.VerifyKeyExists(changeVectorSlice) == false)
                        {
                            _documentsStorage.AttachmentsStorage.RevisionAttachments(context, lowerId, changeVector);
                        }
                    }
                }

                flags |= DocumentFlags.Revision;
                var data = context.ReadObject(document, id);
                var newEtag = _database.DocumentsStorage.GenerateNextEtag();

                using (table.Allocate(out TableValueBuilder tbv))
                {
                    tbv.Add(changeVectorPtr, changeVectorSize);
                    tbv.Add(lowerId);
                    tbv.Add(SpecialChars.RecordSeparator);
                    tbv.Add(Bits.SwapBytes(newEtag));
                    tbv.Add(idPtr);
                    tbv.Add(data.BasePointer, data.Size);
                    tbv.Add((int)flags);
                    tbv.Add(lastModifiedTicks);
                    table.Set(tbv);
                }
            }

            if (notFromSmuggler)
            {
                if (configuration == null)
                {
                    var collectionName = _database.DocumentsStorage.ExtractCollectionName(context, id, document);
                    configuration = GetVersioningConfiguration(collectionName);
                }

                using (GetKeyPrefix(context, lowerId, out Slice prefixSlice))
                {
                    // We delete the old revisions after we put the current one, 
                    // because in case that MaxRevisions is 3 or lower we may get a revision document from replication
                    // which is old. But becasue we put it first, we make sure to clean this document, becuase of the order to the revisions.
                    var revisionsCount = IncrementCountOfRevisions(context, prefixSlice, 1);
                    DeleteOldRevisions(context, table, prefixSlice, configuration.MaxRevisions, revisionsCount);
                }
            }
        }

        private void DeleteOldRevisions(DocumentsOperationContext context, Table table, Slice prefixSlice, long? maxRevisions, long revisionsCount)
        {
            if (maxRevisions.HasValue == false || maxRevisions.Value == int.MaxValue)
                return;

            var numberOfRevisionsToDelete = revisionsCount - maxRevisions.Value;
            if (numberOfRevisionsToDelete <= 0)
                return;

            var deletedRevisionsCount = DeleteRevisions(context, table, prefixSlice, numberOfRevisionsToDelete);
            Debug.Assert(numberOfRevisionsToDelete == deletedRevisionsCount);
            IncrementCountOfRevisions(context, prefixSlice, -deletedRevisionsCount);
        }

        private long DeleteRevisions(DocumentsOperationContext context, Table table, Slice prefixSlice, long numberOfRevisionsToDelete)
        {
            long maxEtagDeleted = 0;

            var deletedRevisionsCount = table.DeleteForwardFrom(DocsSchema.Indexes[IdAndEtagSlice], prefixSlice, true,
                numberOfRevisionsToDelete,
                deleted =>
                {
                    var revision = TableValueToRevision(context, ref deleted.Reader);
                    maxEtagDeleted = Math.Max(maxEtagDeleted, revision.Etag);
                    if ((revision.Flags & DocumentFlags.HasAttachments) == DocumentFlags.HasAttachments)
                    {
                        _documentsStorage.AttachmentsStorage.DeleteRevisionAttachments(context, revision);
                    }
                });
            _database.DocumentsStorage.EnsureLastEtagIsPersisted(context, maxEtagDeleted);
            return deletedRevisionsCount;
        }

        private long IncrementCountOfRevisions(DocumentsOperationContext context, Slice prefixedLowerId, long delta)
        {
            var numbers = context.Transaction.InnerTransaction.ReadTree(RevisionsCountSlice);
            return numbers.Increment(prefixedLowerId, delta);
        }

        private void DeleteCountOfRevisions(DocumentsOperationContext context, Slice prefixedLowerId)
        {
            var numbers = context.Transaction.InnerTransaction.ReadTree(RevisionsCountSlice);
            numbers.Delete(prefixedLowerId);
        }

        public void Delete(DocumentsOperationContext context, CollectionName collectionName, Slice lowerId)
        {
            var configuration = GetVersioningConfiguration(collectionName);
            if (configuration.Active == false)
                return;

            if (configuration.PurgeOnDelete == false)
                return;

            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, RevisionDocumentsSlice);
            using (GetKeyPrefix(context, lowerId, out Slice prefixSlice))
            {
                DeleteRevisions(context, table, prefixSlice, long.MaxValue);
                DeleteCountOfRevisions(context, prefixSlice);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ByteStringContext.InternalScope GetKeyPrefix(DocumentsOperationContext context, Slice lowerId, out Slice prefixSlice)
        {
            return GetKeyPrefix(context, lowerId.Content.Ptr, lowerId.Size, out prefixSlice);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ByteStringContext.InternalScope GetKeyPrefix(DocumentsOperationContext context, byte* lowerId, int lowerIdSize, out Slice prefixSlice)
        {
            var scope = context.Allocator.Allocate(lowerIdSize + 1, out ByteString keyMem);

            Memory.Copy(keyMem.Ptr, lowerId, lowerIdSize);
            keyMem.Ptr[lowerIdSize] = SpecialChars.RecordSeparator;

            prefixSlice = new Slice(SliceOptions.Key, keyMem);
            return scope;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ByteStringContext.InternalScope GetLastKey(DocumentsOperationContext context, Slice lowerId, out Slice prefixSlice)
        {
            var scope = context.Allocator.Allocate(lowerId.Size + 1 + sizeof(long), out ByteString keyMem);

            Memory.Copy(keyMem.Ptr, lowerId.Content.Ptr, lowerId.Size);
            keyMem.Ptr[lowerId.Size] = SpecialChars.RecordSeparator;

            var maxValue = Bits.SwapBytes(long.MaxValue);
            Memory.Copy(keyMem.Ptr + lowerId.Size + 1, (byte*)&maxValue, sizeof(long));

            prefixSlice = new Slice(SliceOptions.Key, keyMem);
            return scope;
        }

        private long CountOfRevisions(DocumentsOperationContext context, Slice prefix)
        {
            var numbers = context.Transaction.InnerTransaction.ReadTree(RevisionsCountSlice);
            return numbers.Read(prefix)?.Reader.ReadLittleEndianInt64() ?? 0;
        }

        public (Document[] Revisions, long Count) GetRevisions(DocumentsOperationContext context, string id, int start, int take)
        {
            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
            using (GetKeyPrefix(context, lowerId, out Slice prefixSlice))
            using (GetLastKey(context, lowerId, out Slice lastKey))
            {
                var revisions = GetRevisions(context, prefixSlice, lastKey, start, take).ToArray();
                var count = CountOfRevisions(context, prefixSlice);
                return (revisions, count);
            }
        }

        private IEnumerable<Document> GetRevisions(DocumentsOperationContext context, Slice prefixSlice, Slice lastKey, int start, int take)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, RevisionDocumentsSlice);
            foreach (var tvr in table.SeekBackwardFrom(DocsSchema.Indexes[IdAndEtagSlice], prefixSlice, lastKey, start))
            {
                if (take-- <= 0)
                    yield break;

                var document = TableValueToRevision(context, ref tvr.Result.Reader);
                yield return document;
            }
        }

        public IEnumerable<Document> GetRevisionsFrom(DocumentsOperationContext context, long etag, int take)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, RevisionDocumentsSlice);

            foreach (var tvr in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[RevisionsEtagsSlice], etag, 0))
            {
                var document = TableValueToRevision(context, ref tvr.Reader);
                yield return document;

                if (take-- <= 0)
                    yield break;
            }
        }

        public IEnumerable<ReplicationBatchItem> GetRevisionsAfter(DocumentsOperationContext context, long etag)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, RevisionDocumentsSlice);

            foreach (var tvr in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[RevisionsEtagsSlice], etag, 0))
            {
                yield return ReplicationBatchItem.From(TableValueToRevision(context, ref tvr.Reader));
            }
        }

        private Document TableValueToRevision(DocumentsOperationContext context, ref TableValueReader tvr)
        {
            var result = new Document
            {
                StorageId = tvr.Id,
                LowerId = DocumentsStorage.TableValueToString(context, (int)Columns.LowerId, ref tvr),
                Id = DocumentsStorage.TableValueToId(context, (int)Columns.Id, ref tvr),
                Etag = DocumentsStorage.TableValueToEtag((int)Columns.Etag, ref tvr)
            };

            int size;
            result.Data = new BlittableJsonReaderObject(tvr.Read((int)Columns.Document, out size), size, context);
            result.LastModified = new DateTime(*(long*)tvr.Read((int)Columns.LastModified, out size));

            var ptr = tvr.Read((int)Columns.ChangeVector, out size);
            int changeVectorCount = size / sizeof(ChangeVectorEntry);
            result.ChangeVector = new ChangeVectorEntry[changeVectorCount];
            for (var i = 0; i < changeVectorCount; i++)
            {
                result.ChangeVector[i] = ((ChangeVectorEntry*)ptr)[i];
            }

            result.Flags = *(DocumentFlags*)tvr.Read((int)Columns.Flags, out size);

            return result;
        }

        public long GetNumberOfRevisionDocuments(DocumentsOperationContext context)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, RevisionDocumentsSlice);
            return table.GetNumberEntriesFor(DocsSchema.FixedSizeIndexes[RevisionsEtagsSlice]);
        }
    }
}