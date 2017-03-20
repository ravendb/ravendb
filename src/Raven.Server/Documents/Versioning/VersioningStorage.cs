using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.Replication;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Versioning
{
    public unsafe class VersioningStorage
    {
        private static readonly Slice KeyAndChangeVectorSlice;
        public static readonly Slice KeyAndEtagSlice;
        public static readonly Slice RevisionsEtagsSlice;
        public static readonly Slice RevisionDocumentsSlice;
        public static readonly Slice RevisionsCountSlice;
        private static Logger _logger;

        private static readonly TableSchema DocsSchema;

        private readonly DocumentDatabase _database;
        private readonly DocumentsStorage _documentsStorage;
        private readonly VersioningConfiguration _versioningConfiguration;

        internal VersioningConfiguration VersioningConfiguration => _versioningConfiguration;
        
        // The documents schema is as follows
        // (lowered key, recored separator, etag, lowered key, recored separator, change vector, lazy string key, document)
        // We are you using the record separator in order to avoid loading another documents that has the same key prefix, 
        //      e.g. fitz(record-separator)01234567 and fitz0(record-separator)01234567, without the record separator we would have to load also fitz0 and filter it.
        // format of lazy string key is detailed in GetLowerKeySliceAndStorageKey

        // We have to fetch versions based on it's change vector as well as by it's insert order.
        // To do so, we duplicate the key and indexing both (key,etag) and (key,cv)
        private enum Columns
        {
            ChangeVector = 0,
            LoweredKey = 1,
            RecordSeparator = 2,
            Etag = 3, // etag to keep the insertion order
            Key = 4,
            Document = 5,
            Flags = 6,
        }

        public const byte RecordSeperator = 30;
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
            Slice.From(StorageEnvironment.LabelsContext, "KeyAndChangeVector", ByteStringType.Immutable, out KeyAndChangeVectorSlice);
            Slice.From(StorageEnvironment.LabelsContext, "KeyAndEtag", ByteStringType.Immutable, out KeyAndEtagSlice);
            Slice.From(StorageEnvironment.LabelsContext, "RevisionsEtags", ByteStringType.Immutable, out RevisionsEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "RevisionDocuments", ByteStringType.Immutable, out RevisionDocumentsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "RevisionsCount", ByteStringType.Immutable, out RevisionsCountSlice);

            DocsSchema = new TableSchema();
            DocsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)Columns.ChangeVector,
                Count = 1,
                Name = KeyAndChangeVectorSlice
            });
            DocsSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)Columns.LoweredKey,
                Count = 3,
                Name = KeyAndEtagSlice
            });
            DocsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)Columns.Etag,
                Name = RevisionsEtagsSlice
            });
        }

        public static VersioningStorage LoadConfigurations(DocumentDatabase database, ServerStore serverStore, VersioningStorage versioningStorage)
        {
            TransactionOperationContext context;
            using (serverStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();
                var dbDoc = serverStore.Cluster.ReadDatabase(context, database.Name);
                if (dbDoc == null)
                    return null;
                try
                {
                    var versioningConfiguration = dbDoc.VersioningConfiguration;
                    if (versioningConfiguration == null)
                        return null;
                    if (versioningConfiguration.Equals(versioningStorage?.VersioningConfiguration))
                        return versioningStorage;                    
                    var config = new VersioningStorage(database, versioningConfiguration);
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Versioning configuration changed");
                    return config;
                }
                catch (Exception e)
                {
                    //TODO: This should generate an alert, so admin will know that something is very bad
                    //TODO: Or this should throw and we should have a config flag to ignore the error
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations(
                            $"Cannot enable versioning for documents as the versioning configuration in the database record is missing or not valid: {dbDoc}",
                            e);
                    return null;
                }
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

        public bool ShouldVersionDocument(CollectionName collectionName,
            NonPersistentDocumentFlags nonPersistentFlags,
            Func<Document> getExistingDocument,
            BlittableJsonReaderObject document,
            ref DocumentFlags documentFlags,
            out VersioningConfigurationCollection configuration)
        {
            configuration = GetVersioningConfiguration(collectionName);
            if (configuration.Active == false)
                return false;

            try
            {
                if ((nonPersistentFlags & NonPersistentDocumentFlags.FromSmuggler) != NonPersistentDocumentFlags.FromSmuggler)
                    return true;

                var existingDocument = getExistingDocument();
                if (existingDocument == null)
                {
                    // we are not going to create a revision if it's an import from v3
                    // (since this import is going to import revisions as well)
                    return (nonPersistentFlags & NonPersistentDocumentFlags.LegacyVersioned) != NonPersistentDocumentFlags.LegacyVersioned;
                }

                // compare the contents of the existing and the new document
                if (existingDocument.IsMetadataEqualTo(document) && existingDocument.IsEqualTo(document))
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

        public void PutFromDocument(DocumentsOperationContext context, string key, BlittableJsonReaderObject document, 
            DocumentFlags flags, ChangeVectorEntry[] changeVector, VersioningConfigurationCollection configuration = null)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, RevisionDocumentsSlice);

            byte* lowerKey;
            int lowerKeySize;
            PutInternal(context, key, document, flags, table, changeVector, out lowerKey, out lowerKeySize);

            if ((flags & DocumentFlags.HasAttachments) == DocumentFlags.HasAttachments)
            {
                _documentsStorage.AttachmentsStorage.RevisionAttachments(context, lowerKey, lowerKeySize, changeVector);
            }

            if (configuration == null)
            {
                var collectionName = _database.DocumentsStorage.ExtractCollectionName(context, key, document);
                configuration = GetVersioningConfiguration(collectionName);
            }

            Slice prefixSlice;
            using (DocumentKeyWorker.GetSliceFromKey(context, key, out prefixSlice))
            {
                // We delete the old revisions after we put the current one, 
                // because in case that MaxRevisions is 3 or lower we may get a revision document from replication
                // which is old. But becasue we put it first, we make sure to clean this document, becuase of the order to the revisions.
                var revisionsCount = IncrementCountOfRevisions(context, prefixSlice, 1);
                DeleteOldRevisions(context, table, prefixSlice, configuration.MaxRevisions, revisionsCount);
            }
        }

        public void PutDirect(DocumentsOperationContext context, string key, BlittableJsonReaderObject document,
            DocumentFlags flags, ChangeVectorEntry[] changeVector)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, RevisionDocumentsSlice);
            byte* lowerKey;
            int lowerKeySize;
            PutInternal(context, key, document, flags, table, changeVector, out lowerKey, out lowerKeySize);
        }

        private void PutInternal(DocumentsOperationContext context, string key, BlittableJsonReaderObject document, 
            DocumentFlags flags, Table table, ChangeVectorEntry[] changeVector, out byte* lowerKey, out int lowerSize)
        {
            BlittableJsonReaderObject.AssertNoModifications(document, key, assertChildren: true);

            flags |= DocumentFlags.FromVersionStorage;

            byte* keyPtr;
            int keySize;
            DocumentKeyWorker.GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);

            var data = context.ReadObject(document, key);

            var newEtag = _database.DocumentsStorage.GenerateNextEtag();

            if (changeVector == null)
            {
                changeVector = new[]
                {
                    new ChangeVectorEntry
                    {
                        DbId = _database.DbId,
                        Etag = newEtag
                    }
                };
            }

            fixed (ChangeVectorEntry* pChangeVector = changeVector)
            {
                TableValueBuilder tbv;
                using (table.Allocate(out tbv))
                {
                    tbv.Add((byte*)pChangeVector, sizeof(ChangeVectorEntry) * changeVector.Length);
                    tbv.Add(lowerKey, lowerSize);
                    tbv.Add(RecordSeperator);
                    tbv.Add(Bits.SwapBytes(newEtag));
                    tbv.Add(keyPtr, keySize);
                    tbv.Add(data.BasePointer, data.Size);
                    tbv.Add((int)flags);
                    table.Set(tbv);
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

        private long DeleteRevisions(DocumentsOperationContext context, Table table, Slice prefixSlice,
            long numberOfRevisionsToDelete)
        {
            long maxEtagDeleted = 0;

            var deletedRevisionsCount = table.DeleteForwardFrom(DocsSchema.Indexes[KeyAndEtagSlice], prefixSlice,
                numberOfRevisionsToDelete,
                deleted =>
                {
                    var revision = TableValueToDocument(context, ref deleted.Reader);
                    maxEtagDeleted = Math.Max(maxEtagDeleted, revision.Etag);
                    if ((revision.Flags & DocumentFlags.HasAttachments) == DocumentFlags.HasAttachments)
                    {
                        _documentsStorage.AttachmentsStorage.DeleteRevisionAttachments(context, revision);
                    }
                });
            _database.DocumentsStorage.EnsureLastEtagIsPersisted(context, maxEtagDeleted);
            return deletedRevisionsCount;
        }

        private long IncrementCountOfRevisions(DocumentsOperationContext context, Slice prefixedLoweredKey, long delta)
        {
            var numbers = context.Transaction.InnerTransaction.ReadTree(RevisionsCountSlice);
            return numbers.Increment(prefixedLoweredKey, delta);
        }

        private void DeleteCountOfRevisions(DocumentsOperationContext context, Slice prefixedLoweredKey)
        {
            var numbers = context.Transaction.InnerTransaction.ReadTree(RevisionsCountSlice);
            numbers.Delete(prefixedLoweredKey);
        }

        public void Delete(DocumentsOperationContext context, CollectionName collectionName, Slice loweredKey)
        {
            var configuration = GetVersioningConfiguration(collectionName);
            if (configuration.Active == false)
                return;

            if (configuration.PurgeOnDelete == false)
                return;

            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, RevisionDocumentsSlice);
            Slice prefixSlice;
            using (GetKeyPrefix(context, loweredKey, out prefixSlice))
            {
                DeleteRevisions(context, table, prefixSlice, long.MaxValue);
                DeleteCountOfRevisions(context, prefixSlice);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ReleaseMemory GetKeyPrefix(DocumentsOperationContext context, Slice loweredKey, out Slice prefixSlice)
        {
            var keyMem = context.Allocator.Allocate(loweredKey.Size + 1);

            loweredKey.CopyTo(0, keyMem.Ptr, 0, loweredKey.Size);
            keyMem.Ptr[loweredKey.Size] = RecordSeperator;

            prefixSlice = new Slice(SliceOptions.Key, keyMem);
            return new ReleaseMemory(keyMem, context);
        }

        public IEnumerable<Document> GetRevisions(DocumentsOperationContext context, string key, int start, int take)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, RevisionDocumentsSlice);

            Slice prefixSlice;
            using (DocumentKeyWorker.GetSliceFromKey(context, key, out prefixSlice))
            {
                // ReSharper disable once LoopCanBeConvertedToQuery
                foreach (var tvr in table.SeekForwardFrom(DocsSchema.Indexes[KeyAndEtagSlice], prefixSlice, start, startsWith: true))
                {
                    if (take-- <= 0)
                        yield break;

                    var document = TableValueToDocument(context, ref tvr.Result.Reader);
                    yield return document;
                }
            }
        }

        public IEnumerable<Document> GetRevisionsAfter(DocumentsOperationContext context, long etag, int take)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, RevisionDocumentsSlice);

            foreach (var tvr in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[RevisionsEtagsSlice], etag, 0))
            {
                var document = TableValueToDocument(context, ref tvr.Reader);
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
                yield return ReplicationBatchItem.From(TableValueToDocument(context, ref tvr.Reader));
            }
        }

        private Document TableValueToDocument(DocumentsOperationContext context, ref TableValueReader tvr)
        {
            var result = new Document
            {
                StorageId = tvr.Id
            };
            result.LoweredKey = DocumentsStorage.TableValueToString(context, (int)Columns.LoweredKey, ref tvr);
            result.Key = DocumentsStorage.TableValueToKey(context, (int)Columns.Key, ref tvr);
            result.Etag = DocumentsStorage.TableValueToEtag((int)Columns.Etag, ref tvr);

            int size;
            result.Data = new BlittableJsonReaderObject(tvr.Read((int)Columns.Document, out size), size, context);

            var ptr = tvr.Read((int)Columns.ChangeVector, out size);
            int changeVecotorCount = size / sizeof(ChangeVectorEntry);
            result.ChangeVector = new ChangeVectorEntry[changeVecotorCount];
            for (var i = 0; i < changeVecotorCount; i++)
            {
                result.ChangeVector[i] = ((ChangeVectorEntry*)ptr)[i];
            }

            result.Flags = *(DocumentFlags*)tvr.Read((int)Columns.Flags, out size);
            if ((result.Flags & DocumentFlags.HasAttachments) == DocumentFlags.HasAttachments)
            {
                Slice prefixSlice;
                using (_documentsStorage.AttachmentsStorage.GetAttachmentPrefix(context, result.LoweredKey.Buffer, result.LoweredKey.Size,
                    AttachmentType.Revision, result.ChangeVector, out prefixSlice))
                {
                    result.Attachments = _documentsStorage.AttachmentsStorage.GetAttachmentsForDocument(context, prefixSlice.Clone(context.Allocator));
                }
            }

            return result;
        }

        public long GetNumberOfRevisionDocuments(DocumentsOperationContext context)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, RevisionDocumentsSlice);
            return table.GetNumberEntriesFor(DocsSchema.FixedSizeIndexes[RevisionsEtagsSlice]);
        }
    }
}