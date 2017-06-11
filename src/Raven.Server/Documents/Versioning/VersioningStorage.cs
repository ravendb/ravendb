using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Server;
using Raven.Client.Server.Versioning;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Utils;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents.Versioning
{
    public unsafe class VersioningStorage
    {
        private static readonly Slice IdAndEtagSlice;
        private static readonly Slice FlagsAndEtagSlice;
        public static readonly Slice AllRevisionsEtagsSlice;
        private static readonly Slice CollectionRevisionsEtagsSlice;
        private static readonly Slice RevisionsCountSlice;
        private static readonly Slice DeleteRevisionSlice;

        private static readonly TableSchema DocsSchema;

        private readonly DocumentDatabase _database;
        private readonly DocumentsStorage _documentsStorage;
        public VersioningConfiguration Configuration { get; private set; }
        private readonly HashSet<string> _tableCreated = new HashSet<string>();
        private readonly Logger _logger;

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
            Etag2 = 7, // Needed to get the zombied revisions with a consistent order
            LastModified = 8,
            Collection = 9,
        }

        private readonly VersioningConfigurationCollection _emptyConfiguration = new VersioningConfigurationCollection();

        public VersioningStorage(DocumentDatabase database)
        {
            _database = database;
            _documentsStorage = _database.DocumentsStorage;
            _logger = LoggingSource.Instance.GetLogger<VersioningStorage>(database.Name);
        }

        private Table EnsureRevisionTableCreated(Transaction tx ,CollectionName collection)
        {
            var tableName = collection.GetTableName(CollectionTableType.Revisions);
            if (_tableCreated.Add(collection.Name))
                DocsSchema.Create(tx, tableName, 16);
            return tx.OpenTable(DocsSchema, tableName);
        }

        static VersioningStorage()
        {
            Slice.From(StorageEnvironment.LabelsContext, "RevisionsChangeVector", ByteStringType.Immutable, out var changeVectorSlice);
            Slice.From(StorageEnvironment.LabelsContext, "RevisionsIdAndEtag", ByteStringType.Immutable, out IdAndEtagSlice);
            Slice.From(StorageEnvironment.LabelsContext, "RevisionsFlagsAndEtag", ByteStringType.Immutable, out FlagsAndEtagSlice);
            Slice.From(StorageEnvironment.LabelsContext, "AllRevisionsEtags", ByteStringType.Immutable, out AllRevisionsEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "CollectionRevisionsEtags", ByteStringType.Immutable, out CollectionRevisionsEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "RevisionsCount", ByteStringType.Immutable, out RevisionsCountSlice);
            var deleteRevision = DocumentFlags.DeleteRevision;
            Slice.From(StorageEnvironment.LabelsContext, (byte*)&deleteRevision, sizeof(DocumentFlags), ByteStringType.Immutable, out DeleteRevisionSlice);

            DocsSchema = new TableSchema();
            DocsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)Columns.ChangeVector,
                Count = 1,
                Name = changeVectorSlice,
                IsGlobal = true
            });
            DocsSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)Columns.LowerId,
                Count = 3,
                Name = IdAndEtagSlice,
                IsGlobal = true
            });
            DocsSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)Columns.Flags,
                Count = 2,
                Name = FlagsAndEtagSlice,
                IsGlobal = true
            });
            DocsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)Columns.Etag,
                Name = AllRevisionsEtagsSlice,
                IsGlobal = true
            });
            DocsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)Columns.Etag,
                Name = CollectionRevisionsEtagsSlice,
            });
        }

        public void InitializeFromDatabaseRecord(DatabaseRecord dbRecord)
        {
            try
            {
                if (dbRecord.Versioning == null)
                {
                    Configuration = null;
                    return;
                }

                if (dbRecord.Versioning.Equals(Configuration))
                    return;

                Configuration = dbRecord.Versioning;

                using (var tx = _database.DocumentsStorage.Environment.WriteTransaction())
                {
                    foreach (var collection in Configuration.Collections)
                    {
                        if (collection.Value.Active == false)
                            continue;
                        EnsureRevisionTableCreated(tx, new CollectionName(collection.Key));
                    }

                    tx.CreateTree(RevisionsCountSlice);
                    tx.Commit();
                }

                if (_logger.IsInfoEnabled)
                    _logger.Info("Versioning configuration changed");
            }
            catch (Exception e)
            {
                var msg = "Cannot enable versioning for documents as the versioning configuration" +
                          $" in the database record is missing or not valid: {dbRecord}";
                _database.NotificationCenter.Add(AlertRaised.Create($"Versioning error in {_database.Name}", msg,
                    AlertType.VersioningConfigurationNotValid, NotificationSeverity.Error, _database.Name));
                if (_logger.IsOperationsEnabled)
                    _logger.Operations(msg, e);
            }
        }

        public bool IsVersioned(string collection)
        {
            if (Configuration.Collections != null && Configuration.Collections.TryGetValue(collection, out var configuration))
            {
                return configuration.Active;
            }

            if (Configuration.Default != null)
            {
                return Configuration.Default.Active;
            }

            return _emptyConfiguration.Active;
        }

        private VersioningConfigurationCollection GetVersioningConfiguration(CollectionName collectionName)
        {
            if (Configuration.Collections != null && 
                Configuration.Collections.TryGetValue(collectionName.Name, out VersioningConfigurationCollection configuration))
            {
                return configuration;
            }

            if (Configuration.Default != null)
            {
                return Configuration.Default;
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

            var collectionName = _database.DocumentsStorage.ExtractCollectionName(context, id, document);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, id, out Slice lowerId, out Slice idPtr))
            using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
            {
                var notFromSmuggler = (nonPersistentFlags & NonPersistentDocumentFlags.FromSmuggler) != NonPersistentDocumentFlags.FromSmuggler;

                var table = EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);

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
                    var newEtagSwapBytes = Bits.SwapBytes(newEtag);

                    using (table.Allocate(out TableValueBuilder tbv))
                    {
                        tbv.Add(changeVectorPtr, changeVectorSize);
                        tbv.Add(lowerId);
                        tbv.Add(SpecialChars.RecordSeparator);
                        tbv.Add(newEtagSwapBytes);
                        tbv.Add(idPtr);
                        tbv.Add(data.BasePointer, data.Size);
                        tbv.Add((int)flags);
                        tbv.Add(newEtagSwapBytes);
                        tbv.Add(lastModifiedTicks);
                        tbv.Add(collectionSlice);
                        var isNew = table.Set(tbv);
                        if (isNew == false)
                            // It might be just an update from replication as we call this twice, both for the doc delete and for deleteRevision.
                            return;
                    }
                }

                if (configuration == null)
                    configuration = GetVersioningConfiguration(collectionName);

                DeleteOldRevisions(context, table, lowerId, configuration.MaxRevisions, nonPersistentFlags);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DeleteOldRevisions(DocumentsOperationContext context, Table table, Slice lowerId, 
            long? maxRevisions, NonPersistentDocumentFlags nonPersistentFlags)
        {
            using (GetKeyPrefix(context, lowerId, out Slice prefixSlice))
            {
                // We delete the old revisions after we put the current one, 
                // because in case that MaxRevisions is 3 or lower we may get a revision document from replication
                // which is old. But because we put it first, we make sure to clean this document, because of the order to the revisions.
                var revisionsCount = IncrementCountOfRevisions(context, prefixSlice, 1);
                DeleteOldRevisions(context, table, prefixSlice, maxRevisions, revisionsCount, nonPersistentFlags);
            }
        }

        private void DeleteOldRevisions(DocumentsOperationContext context, Table table, Slice prefixSlice, long? maxRevisions, long revisionsCount, 
            NonPersistentDocumentFlags nonPersistentFlags)
        {
            if ((nonPersistentFlags & NonPersistentDocumentFlags.FromSmuggler) == NonPersistentDocumentFlags.FromSmuggler)
                return;

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

        public void Delete(DocumentsOperationContext context, CollectionName collectionName, string id, Slice lowerId, ChangeVectorEntry[] changeVector,
            long lastModifiedTicks, NonPersistentDocumentFlags nonPersistentFlags)
        {
            using (DocumentIdWorker.GetStringPreserveCase(context, id, out Slice idPtr))
            {
                Delete(context, collectionName, lowerId, idPtr, changeVector, lastModifiedTicks, nonPersistentFlags);
            }
        }

        public void Delete(DocumentsOperationContext context, string collectionName, string id, ChangeVectorEntry[] changeVector,
            long lastModifiedTicks, NonPersistentDocumentFlags nonPersistentFlags)
        {
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, id, out Slice lowerId, out Slice idPtr))
            {
                var collection = _documentsStorage.ExtractCollectionName(context, collectionName);
                Delete(context, collection, lowerId, idPtr, changeVector, lastModifiedTicks, nonPersistentFlags);
            }
        }

        public void Delete(DocumentsOperationContext context, CollectionName collectionName, Slice lowerId, Slice id, ChangeVectorEntry[] changeVector, 
            long lastModifiedTicks, NonPersistentDocumentFlags nonPersistentFlags)
        {
            var configuration = GetVersioningConfiguration(collectionName);
            if (configuration.Active == false)
                return;

            var table = EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);

            if (configuration.PurgeOnDelete)
            {
                using (GetKeyPrefix(context, lowerId, out Slice prefixSlice))
                {
                    DeleteRevisions(context, table, prefixSlice, long.MaxValue);
                    DeleteCountOfRevisions(context, prefixSlice);
                }

                return;
            }

            var newEtag = _database.DocumentsStorage.GenerateNextEtag();
            var newEtagSwapBytes = Bits.SwapBytes(newEtag);

            fixed (ChangeVectorEntry* pChangeVector = changeVector)
            {
                var changeVectorPtr = (byte*)pChangeVector;
                var changeVectorSize = sizeof(ChangeVectorEntry) * changeVector.Length;

                using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
                {
                    using (table.Allocate(out TableValueBuilder tbv))
                    {
                        tbv.Add(changeVectorPtr, changeVectorSize);
                        tbv.Add(lowerId);
                        tbv.Add(SpecialChars.RecordSeparator);
                        tbv.Add(newEtagSwapBytes);
                        tbv.Add(id);
                        tbv.Add(null, 0);
                        tbv.Add((int)DocumentFlags.DeleteRevision);
                        tbv.Add(newEtagSwapBytes);
                        tbv.Add(lastModifiedTicks);
                        tbv.Add(collectionSlice);
                        var isNew = table.Set(tbv);
                        if (isNew == false)
                            // It might be just an update from replication as we call this twice, both for the doc delete and for deleteRevision.
                            return;

                    }
                }
            }

            DeleteOldRevisions(context, table, lowerId, configuration.MaxRevisions, nonPersistentFlags);
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
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);
            foreach (var tvr in table.SeekBackwardFrom(DocsSchema.Indexes[IdAndEtagSlice], prefixSlice, lastKey, start))
            {
                if (take-- <= 0)
                    yield break;

                var document = TableValueToRevision(context, ref tvr.Result.Reader);
                yield return document;
            }
        }

        public IEnumerable<Document> GetZombiedRevisions(DocumentsOperationContext context, long startEtag, int take)
        {
            using (GetZombiedRevisionKey(context, startEtag, out Slice zombiedKey))
            {
                var table = new Table(DocsSchema, context.Transaction.InnerTransaction);
                foreach (var tvr in table.SeekBackwardFrom(DocsSchema.Indexes[FlagsAndEtagSlice], DeleteRevisionSlice, zombiedKey))
                {
                    if (take-- <= 0)
                        yield break;

                    var etag = DocumentsStorage.TableValueToEtag((int)Columns.Etag, ref tvr.Result.Reader);
                    using (DocumentsStorage.TableValueToSlice(context, (int)Columns.LowerId, ref tvr.Result.Reader, out Slice lowerId))
                    {
                        if (IsZombiedRevision(context, table, lowerId, etag) == false)
                            continue;
                    }

                    yield return TableValueToRevision(context, ref tvr.Result.Reader);
                }
            }
        }

        private bool IsZombiedRevision(DocumentsOperationContext context, Table table, Slice lowerId, long zombiedEtag)
        {
            using (GetKeyPrefix(context, lowerId, out Slice prefixSlice))
            using (GetLastKey(context, lowerId, out Slice lastKey))
            {
                var tvr = table.SeekOneBackwardFrom(DocsSchema.Indexes[IdAndEtagSlice], prefixSlice, lastKey);
                if (tvr == null)
                {
                    Debug.Assert(false, "Cannot happen.");
                    return true;
                }

                var etag = DocumentsStorage.TableValueToEtag((int)Columns.Etag, ref tvr.Reader);
                var flags = DocumentsStorage.TableValueToFlags((int)Columns.Flags, ref tvr.Reader);
                Debug.Assert(zombiedEtag <= etag, "Zombied etag candidate cannot meet a bigger etag.");
                return flags == DocumentFlags.DeleteRevision && zombiedEtag >= etag;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ByteStringContext<ByteStringMemoryCache>.InternalScope GetZombiedRevisionKey(DocumentsOperationContext context, long etag, out Slice zombiedKey)
        {
            var scope = context.Allocator.Allocate(sizeof(DocumentFlags) + sizeof(long), out ByteString keyMem);

            var zombiedRevision = DocumentFlags.DeleteRevision;
            Memory.Copy(keyMem.Ptr, (byte*)&zombiedRevision, sizeof(DocumentFlags));

            var swapBytesEtag = Bits.SwapBytes(etag);
            Memory.Copy(keyMem.Ptr + sizeof(DocumentFlags), (byte*)&swapBytesEtag, sizeof(long));

            zombiedKey = new Slice(SliceOptions.Key, keyMem);
            return scope;
        }

        public IEnumerable<Document> GetRevisionsFrom(DocumentsOperationContext context, long etag, int take)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            foreach (var tvr in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[AllRevisionsEtagsSlice], etag, 0))
            {
                var document = TableValueToRevision(context, ref tvr.Reader);
                yield return document;

                if (take-- <= 0)
                    yield break;
            }
        }
        
        public IEnumerable<(Document previous, Document current)> GetRevisionsFrom(DocumentsOperationContext context, CollectionName collectionName, long etag, int take)
        {
            var table = EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);
            var docsSchemaIndex = DocsSchema.Indexes[IdAndEtagSlice];
            
            foreach (var tvr in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[CollectionRevisionsEtagsSlice], etag, 0))
            {
                if (take-- <= 0)
                    break;
                var current = TableValueToRevision(context, ref tvr.Reader);

                using (docsSchemaIndex.GetSlice(context.Allocator, ref tvr.Reader, out var idAndEtag))
                using (Slice.External(context.Allocator, idAndEtag, idAndEtag.Size - sizeof(long), out var prefix))
                {
                    bool hasPrevious = false;
                    foreach (var prevTvr in table.SeekBackwardFrom(docsSchemaIndex, prefix, idAndEtag, 1))
                    {
                        var previous = TableValueToRevision(context, ref prevTvr.Result.Reader);
                        yield return (previous, current);
                        hasPrevious = true;
                        break;
                    }
                    if (hasPrevious)
                        continue;
                }

                yield return (null, current);
            }
        }

        private Document TableValueToRevision(DocumentsOperationContext context, ref TableValueReader tvr)
        {
            var result = new Document
            {
                StorageId = tvr.Id,
                LowerId = DocumentsStorage.TableValueToString(context, (int)Columns.LowerId, ref tvr),
                Id = DocumentsStorage.TableValueToId(context, (int)Columns.Id, ref tvr),
                Etag = DocumentsStorage.TableValueToEtag((int)Columns.Etag, ref tvr),
                Collection = DocumentsStorage.TableValueToId(context, (int)Columns.Collection, ref tvr)
            };

            var ptr = tvr.Read((int)Columns.Document, out int size);
            if (size > 0)
                result.Data = new BlittableJsonReaderObject(ptr, size, context);

            result.LastModified = new DateTime(*(long*)tvr.Read((int)Columns.LastModified, out size));

            ptr = tvr.Read((int)Columns.ChangeVector, out size);
            var changeVectorCount = size / sizeof(ChangeVectorEntry);
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
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);
            return table.GetNumberOfEntriesFor(DocsSchema.FixedSizeIndexes[AllRevisionsEtagsSlice]);
        }
    }
}