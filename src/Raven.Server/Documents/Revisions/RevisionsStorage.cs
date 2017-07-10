using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Raven.Client;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Server;
using Raven.Client.Server.Revisions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Utils;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using static Raven.Server.Documents.DocumentsStorage;

namespace Raven.Server.Documents.Revisions
{
    public unsafe class RevisionsStorage
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
        public RevisionsConfiguration Configuration { get; private set; }
        public readonly RevisionsOperations Operations;
        private readonly HashSet<string> _tableCreated = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
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
            Etag2 = 7, // Needed to get the revisions bin entries with a consistent order
            LastModified = 8,
        }

        private readonly RevisionsCollectionConfiguration _emptyConfiguration = new RevisionsCollectionConfiguration();

        public RevisionsStorage(DocumentDatabase database)
        {
            _database = database;
            _documentsStorage = _database.DocumentsStorage;
            _logger = LoggingSource.Instance.GetLogger<RevisionsStorage>(database.Name);
            Operations = new RevisionsOperations(_database);
        }

        private Table EnsureRevisionTableCreated(Transaction tx, CollectionName collection)
        {
            var tableName = collection.GetTableName(CollectionTableType.Revisions);
            if (_tableCreated.Add(collection.Name))
                DocsSchema.Create(tx, tableName, 16);
            return tx.OpenTable(DocsSchema, tableName);
        }

        static RevisionsStorage()
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
                var revisions = dbRecord.Revisions;
                if (revisions == null ||
                    (revisions.Default == null && revisions.Collections.Count == 0))
                {
                    Configuration = null;
                    return;
                }

                if (revisions.Equals(Configuration))
                    return;

                Configuration = revisions;

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
                    _logger.Info("Revisions configuration changed");
            }
            catch (Exception e)
            {
                var msg = "Cannot enable revisions for documents as the revisions configuration" +
                          $" in the database record is missing or not valid: {dbRecord}";
                _database.NotificationCenter.Add(AlertRaised.Create($"Revisions error in {_database.Name}", msg,
                    AlertType.RevisionsConfigurationNotValid, NotificationSeverity.Error, _database.Name));
                if (_logger.IsOperationsEnabled)
                    _logger.Operations(msg, e);
            }
        }

        public bool IsVersioned(string collection)
        {
            if (Configuration == null)
                return false;

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

        private RevisionsCollectionConfiguration GetRevisionsConfiguration(CollectionName collectionName)
        {
            if (Configuration.Collections != null && 
                Configuration.Collections.TryGetValue(collectionName.Name, out RevisionsCollectionConfiguration configuration))
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
            out RevisionsCollectionConfiguration configuration)
        {
            configuration = GetRevisionsConfiguration(collectionName);
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
            RevisionsCollectionConfiguration configuration = null)
        {
            Debug.Assert(changeVector != null, "Change vector must be set");

            BlittableJsonReaderObject.AssertNoModifications(document, id, assertChildren: true);

            var collectionName = _database.DocumentsStorage.ExtractCollectionName(context, id, document);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, id, out Slice lowerId, out Slice idPtr))
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

                    using (table.Allocate(out TableValueBuilder tvb))
                    {
                        tvb.Add(changeVectorPtr, changeVectorSize);
                        tvb.Add(lowerId);
                        tvb.Add(SpecialChars.RecordSeparator);
                        tvb.Add(newEtagSwapBytes);
                        tvb.Add(idPtr);
                        tvb.Add(data.BasePointer, data.Size);
                        tvb.Add((int)flags);
                        tvb.Add(newEtagSwapBytes);
                        tvb.Add(lastModifiedTicks);
                        var isNew = table.Set(tvb);
                        if (isNew == false)
                            // It might be just an update from replication as we call this twice, both for the doc delete and for deleteRevision.
                            return;
                    }
                }

                if (configuration == null)
                    configuration = GetRevisionsConfiguration(collectionName);

                DeleteOldRevisions(context, table, lowerId, configuration, nonPersistentFlags, changeVector);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DeleteOldRevisions(DocumentsOperationContext context, Table table, Slice lowerId, 
            RevisionsCollectionConfiguration configuration, NonPersistentDocumentFlags nonPersistentFlags, ChangeVectorEntry[] changeVector)
        {
            using (GetKeyPrefix(context, lowerId, out Slice prefixSlice))
            {
                // We delete the old revisions after we put the current one, 
                // because in case that MinimumRevisionsToKeep is 3 or lower we may get a revision document from replication
                // which is old. But because we put it first, we make sure to clean this document, because of the order to the revisions.
                var revisionsCount = IncrementCountOfRevisions(context, prefixSlice, 1);
                DeleteOldRevisions(context, table, prefixSlice, configuration, revisionsCount, nonPersistentFlags, changeVector);
            }
        }

        private void DeleteOldRevisions(DocumentsOperationContext context, Table table, Slice prefixSlice, 
            RevisionsCollectionConfiguration configuration, long revisionsCount, NonPersistentDocumentFlags nonPersistentFlags, 
            ChangeVectorEntry[] changeVector)
        {
            if ((nonPersistentFlags & NonPersistentDocumentFlags.FromSmuggler) == NonPersistentDocumentFlags.FromSmuggler)
                return;

            if (configuration.MinimumRevisionsToKeep.HasValue == false &&
                configuration.MinimumRevisionAgeToKeep.HasValue == false)
                return;

            var numberOfRevisionsToDelete = revisionsCount - configuration.MinimumRevisionsToKeep ?? 0;
            if (numberOfRevisionsToDelete <= 0)
                return;

            var deletedRevisionsCount = DeleteRevisions(context, table, prefixSlice, numberOfRevisionsToDelete, configuration.MinimumRevisionAgeToKeep, changeVector);
            Debug.Assert(numberOfRevisionsToDelete >= deletedRevisionsCount);
            IncrementCountOfRevisions(context, prefixSlice, -deletedRevisionsCount);
        }

        public void DeleteRevisionsFor(DocumentsOperationContext context, string id)
        {
            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
            using (GetKeyPrefix(context, lowerId, out Slice prefixSlice))
            {
                var collectionName = GetCollectionFor(context, prefixSlice);
                if (collectionName == null)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Tried to delete all revisions for '{id}' but no revisions found.");
                    return;
                }

                var table = EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);
                var newEtag = _documentsStorage.GenerateNextEtag();
                var changeVector = _documentsStorage.GetNewChangeVector(context, newEtag);
                DeleteRevisions(context, table, prefixSlice, long.MaxValue, null, changeVector);
                DeleteCountOfRevisions(context, prefixSlice);
            }
        }

        public void DeleteRevisionsBefore(DocumentsOperationContext context, string collection, DateTime time)
        {
            var collectionName = new CollectionName(collection);
            var table = EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);
            table.DeleteByPrimaryKey(Slices.BeforeAllKeys, deleted =>
            {
                var lastModified = TableValueToDateTime((int)Columns.LastModified, ref deleted.Reader);
                if (lastModified >= time)
                    return false;

                using (TableValueToSlice(context, (int)Columns.LowerId, ref deleted.Reader, out Slice lowerId))
                using (GetKeyPrefix(context, lowerId, out Slice prefixSlice))
                {
                    IncrementCountOfRevisions(context, prefixSlice, -1);
                }
                return true;
            });
        }

        private CollectionName GetCollectionFor(DocumentsOperationContext context, Slice prefixSlice)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);
            var tvr = table.SeekOneForwardFrom(DocsSchema.Indexes[IdAndEtagSlice], prefixSlice);
            if (tvr == null)
                return null;

            var ptr = tvr.Reader.Read((int)Columns.Document, out int size);
            var data = new BlittableJsonReaderObject(ptr, size, context);

            return _documentsStorage.ExtractCollectionName(context, null, data);
        }

        private long DeleteRevisions(DocumentsOperationContext context, Table table, Slice prefixSlice, long numberOfRevisionsToDelete,
            TimeSpan? minimumTimeToKeep, ChangeVectorEntry[] changeVector)
        {
            long maxEtagDeleted = 0;

            var deletedRevisionsCount = table.DeleteForwardFrom(DocsSchema.Indexes[IdAndEtagSlice], prefixSlice, true,
                numberOfRevisionsToDelete,
                deleted =>
                {
                    var revision = TableValueToRevision(context, ref deleted.Reader);

                    if (minimumTimeToKeep.HasValue &&
                        _database.Time.GetUtcNow() - revision.LastModified <= minimumTimeToKeep.Value)
                        return false;

                    maxEtagDeleted = Math.Max(maxEtagDeleted, revision.Etag);
                    if ((revision.Flags & DocumentFlags.HasAttachments) == DocumentFlags.HasAttachments)
                    {
                        _documentsStorage.AttachmentsStorage.DeleteRevisionAttachments(context, revision, changeVector);
                    }
                    return true;
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

        public void Delete(DocumentsOperationContext context, string id, Slice lowerId, CollectionName collectionName, ChangeVectorEntry[] changeVector,
            long lastModifiedTicks, NonPersistentDocumentFlags nonPersistentFlags)
        {
            using (DocumentIdWorker.GetStringPreserveCase(context, id, out Slice idPtr))
            {
                var deleteRevisionDocument = context.ReadObject(new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Collection] = collectionName.Name
                    }
                }, "RevisionsBin");
                Delete(context, lowerId, idPtr, collectionName, deleteRevisionDocument, changeVector, lastModifiedTicks, nonPersistentFlags);
            }
        }

        public void Delete(DocumentsOperationContext context, string id, BlittableJsonReaderObject deleteRevisionDocument, ChangeVectorEntry[] changeVector,
            long lastModifiedTicks, NonPersistentDocumentFlags nonPersistentFlags)
        {
            BlittableJsonReaderObject.AssertNoModifications(deleteRevisionDocument, id, assertChildren: true);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, id, out Slice lowerId, out Slice idPtr))
            {
                var collectionName = _documentsStorage.ExtractCollectionName(context, id, deleteRevisionDocument);
                Delete(context, lowerId, idPtr, collectionName, deleteRevisionDocument, changeVector, lastModifiedTicks, nonPersistentFlags);
            }
        }

        private void Delete(DocumentsOperationContext context, Slice lowerId, Slice id, CollectionName collectionName, 
            BlittableJsonReaderObject deleteRevisionDocument, ChangeVectorEntry[] changeVector, 
            long lastModifiedTicks, NonPersistentDocumentFlags nonPersistentFlags)
        {
            Debug.Assert(changeVector != null, "Change vector must be set");

            var configuration = GetRevisionsConfiguration(collectionName);
            if (configuration.Active == false)
                return;

            var table = EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);

            if (configuration.PurgeOnDelete)
            {
                using (GetKeyPrefix(context, lowerId, out Slice prefixSlice))
                {
                    DeleteRevisions(context, table, prefixSlice, long.MaxValue, null, changeVector);
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

                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(changeVectorPtr, changeVectorSize);
                    tvb.Add(lowerId);
                    tvb.Add(SpecialChars.RecordSeparator);
                    tvb.Add(newEtagSwapBytes);
                    tvb.Add(id);
                    tvb.Add(deleteRevisionDocument.BasePointer, deleteRevisionDocument.Size);
                    tvb.Add((int)DocumentFlags.DeleteRevision);
                    tvb.Add(newEtagSwapBytes);
                    tvb.Add(lastModifiedTicks);
                    var isNew = table.Set(tvb);
                    if (isNew == false)
                        // It might be just an update from replication as we call this twice, both for the doc delete and for deleteRevision.
                        return;
                }
            }

            DeleteOldRevisions(context, table, lowerId, configuration, nonPersistentFlags, changeVector);
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

        public ByteStringContext<ByteStringMemoryCache>.InternalScope GetLatestRevisionsBinEntryEtag(DocumentsOperationContext context, long startEtag, 
            out Slice revisionsBinEntryKey, out long latestEtag)
        {
            var dispose = GetRevisionsBinEntryKey(context, startEtag, out revisionsBinEntryKey);
            foreach (var entry in GetRevisionsBinEntries(context, revisionsBinEntryKey, 1))
            {
                latestEtag = entry.Etag;
                return dispose;
            }

            latestEtag = -1;
            return dispose;
        }

        public IEnumerable<Document> GetRevisionsBinEntries(DocumentsOperationContext context, Slice revisionsBinEntryKey, int take)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);
            foreach (var tvr in table.SeekBackwardFrom(DocsSchema.Indexes[FlagsAndEtagSlice], DeleteRevisionSlice, revisionsBinEntryKey))
            {
                if (take-- <= 0)
                    yield break;

                var etag = TableValueToEtag((int)Columns.Etag, ref tvr.Result.Reader);
                using (TableValueToSlice(context, (int)Columns.LowerId, ref tvr.Result.Reader, out Slice lowerId))
                {
                    if (IsRevisionsBinEntry(context, table, lowerId, etag) == false)
                        continue;
                }

                yield return TableValueToRevision(context, ref tvr.Result.Reader);
            }
        }

        private bool IsRevisionsBinEntry(DocumentsOperationContext context, Table table, Slice lowerId, long revisionsBinEntryEtag)
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

                var etag = TableValueToEtag((int)Columns.Etag, ref tvr.Reader);
                var flags = TableValueToFlags((int)Columns.Flags, ref tvr.Reader);
                Debug.Assert(revisionsBinEntryEtag <= etag, "Revisions bin entry etag candidate cannot meet a bigger etag.");
                return flags == DocumentFlags.DeleteRevision && revisionsBinEntryEtag >= etag;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ByteStringContext<ByteStringMemoryCache>.InternalScope GetRevisionsBinEntryKey(DocumentsOperationContext context, long etag, out Slice deletedRevisionKey)
        {
            var scope = context.Allocator.Allocate(sizeof(DocumentFlags) + sizeof(long), out ByteString keyMem);

            var deleteRevision = DocumentFlags.DeleteRevision;
            Memory.Copy(keyMem.Ptr, (byte*)&deleteRevision, sizeof(DocumentFlags));

            var swapBytesEtag = Bits.SwapBytes(etag);
            Memory.Copy(keyMem.Ptr + sizeof(DocumentFlags), (byte*)&swapBytesEtag, sizeof(long));

            deletedRevisionKey = new Slice(SliceOptions.Key, keyMem);
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
                LowerId = TableValueToString(context, (int)Columns.LowerId, ref tvr),
                Id = TableValueToId(context, (int)Columns.Id, ref tvr),
                Etag = TableValueToEtag((int)Columns.Etag, ref tvr),
                LastModified = TableValueToDateTime((int)Columns.LastModified, ref tvr),
                Flags = TableValueToFlags((int)Columns.Flags, ref tvr)
            };

            var ptr = tvr.Read((int)Columns.Document, out int size);
            result.Data = new BlittableJsonReaderObject(ptr, size, context);

            ptr = tvr.Read((int)Columns.ChangeVector, out size);
            var changeVectorCount = size / sizeof(ChangeVectorEntry);
            result.ChangeVector = new ChangeVectorEntry[changeVectorCount];
            for (var i = 0; i < changeVectorCount; i++)
            {
                result.ChangeVector[i] = ((ChangeVectorEntry*)ptr)[i];
            }

            return result;
        }

        public long GetNumberOfRevisionDocuments(DocumentsOperationContext context)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);
            return table.GetNumberOfEntriesFor(DocsSchema.FixedSizeIndexes[AllRevisionsEtagsSlice]);
        }        
    }
}