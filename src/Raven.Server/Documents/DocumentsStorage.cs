using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Config;
using Raven.Server.Documents.Expiration;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Revisions;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Server.Storage.Layout;
using Raven.Server.Storage.Schema;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Exceptions;
using Voron.Impl;

namespace Raven.Server.Documents
{
    public unsafe class DocumentsStorage : IDisposable
    {
        private static readonly Slice DocsSlice;
        public static readonly Slice CollectionEtagsSlice;
        internal static readonly Slice AllDocsEtagsSlice;
        private static readonly Slice TombstonesSlice;
        public static readonly Slice CollectionsSlice;
        private static readonly Slice LastReplicatedEtagsSlice;
        private static readonly Slice EtagsSlice;
        private static readonly Slice LastEtagSlice;
        private static readonly Slice GlobalTreeSlice;
        private static readonly Slice GlobalChangeVectorSlice;

        private static readonly Slice AllTombstonesEtagsSlice;
        private static readonly Slice TombstonesPrefix;
        private static readonly Slice DeletedEtagsSlice;

        public static readonly TableSchema DocsSchema = new TableSchema
        {
            TableType = (byte)TableType.Documents
        };

        public static readonly TableSchema CompressedDocsSchema = new TableSchema
        {
            TableType = (byte)TableType.Documents
        };

        public static readonly TableSchema TombstonesSchema = new TableSchema();
        public static readonly TableSchema CollectionsSchema = new TableSchema();

        public readonly DocumentDatabase DocumentDatabase;

        private Dictionary<string, CollectionName> _collectionsCache;

        internal enum TombstoneTable
        {
            LowerId = 0,
            Etag = 1,
            DeletedEtag = 2,
            TransactionMarker = 3,
            Type = 4,
            Collection = 5,
            Flags = 6,
            ChangeVector = 7,
            LastModified = 8
        }

        public enum DocumentsTable
        {
            LowerId = 0,
            Etag = 1,
            Id = 2, // format of lazy string id is detailed in GetLowerIdSliceAndStorageKey
            Data = 3,
            ChangeVector = 4,
            LastModified = 5,
            Flags = 6,
            TransactionMarker = 7
        }

        public enum CollectionsTable
        {
            Name = 0,
        }

        static DocumentsStorage()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "AllTombstonesEtags", ByteStringType.Immutable, out AllTombstonesEtagsSlice);
                Slice.From(ctx, "Etags", ByteStringType.Immutable, out EtagsSlice);
                Slice.From(ctx, "LastEtag", ByteStringType.Immutable, out LastEtagSlice);
                Slice.From(ctx, "Docs", ByteStringType.Immutable, out DocsSlice);
                Slice.From(ctx, "CollectionEtags", ByteStringType.Immutable, out CollectionEtagsSlice);
                Slice.From(ctx, "AllDocsEtags", ByteStringType.Immutable, out AllDocsEtagsSlice);
                Slice.From(ctx, "Tombstones", ByteStringType.Immutable, out TombstonesSlice);
                Slice.From(ctx, "Collections", ByteStringType.Immutable, out CollectionsSlice);
                Slice.From(ctx, CollectionName.GetTablePrefix(CollectionTableType.Tombstones), ByteStringType.Immutable, out TombstonesPrefix);
                Slice.From(ctx, "DeletedEtags", ByteStringType.Immutable, out DeletedEtagsSlice);
                Slice.From(ctx, "LastReplicatedEtags", ByteStringType.Immutable, out LastReplicatedEtagsSlice);
                Slice.From(ctx, "GlobalTree", ByteStringType.Immutable, out GlobalTreeSlice);
                Slice.From(ctx, "GlobalChangeVector", ByteStringType.Immutable, out GlobalChangeVectorSlice);
            }
            /*
            Collection schema is:
            full name
            collections are never deleted from the collections table
            */
            CollectionsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)CollectionsTable.Name,
                Count = 1,
                IsGlobal = false
            });

            DefineIndexesForDocsSchema(DocsSchema);
            DefineIndexesForDocsSchema(CompressedDocsSchema);
            DocsSchema.CompressValues(DocsSchema.FixedSizeIndexes[CollectionEtagsSlice], compress: false);
            CompressedDocsSchema.CompressValues(CompressedDocsSchema.FixedSizeIndexes[CollectionEtagsSlice], compress: true);

            TombstonesSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)TombstoneTable.LowerId,
                Count = 1,
                IsGlobal = true,
                Name = TombstonesSlice
            });
            TombstonesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)TombstoneTable.Etag,
                IsGlobal = false,
                Name = CollectionEtagsSlice
            });
            TombstonesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)TombstoneTable.Etag,
                IsGlobal = true,
                Name = AllTombstonesEtagsSlice
            });
            TombstonesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)TombstoneTable.DeletedEtag,
                IsGlobal = false,
                Name = DeletedEtagsSlice
            });

            void DefineIndexesForDocsSchema(TableSchema docsSchema)
            {
                docsSchema.DefineKey(new TableSchema.SchemaIndexDef { StartIndex = (int)DocumentsTable.LowerId, Count = 1, IsGlobal = true, Name = DocsSlice });
                docsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef { StartIndex = (int)DocumentsTable.Etag, IsGlobal = false, Name = CollectionEtagsSlice });
                docsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef { StartIndex = (int)DocumentsTable.Etag, IsGlobal = true, Name = AllDocsEtagsSlice });
            }
        }

        private readonly Logger _logger;
        private readonly string _name;

        // this is only modified by write transactions under lock
        // no need to use thread safe ops
        private long _lastEtag;

        public DocumentsContextPool ContextPool;

        public DocumentsStorage(DocumentDatabase documentDatabase, Action<string> addToInitLog)
        {
            DocumentDatabase = documentDatabase;
            _name = DocumentDatabase.Name;
            _logger = LoggingSource.Instance.GetLogger<DocumentsStorage>(documentDatabase.Name);
            _addToInitLog = addToInitLog;
        }

        public StorageEnvironment Environment { get; private set; }

        public RevisionsStorage RevisionsStorage;
        public ExpirationStorage ExpirationStorage;
        public ConflictsStorage ConflictsStorage;
        public AttachmentsStorage AttachmentsStorage;
        public CountersStorage CountersStorage;
        public TimeSeriesStorage TimeSeriesStorage;
        public DocumentPutAction DocumentPut;
        private readonly Action<string> _addToInitLog;

        // this is used to remember the metadata about collections / documents for
        // common operations. It always points to the latest valid transaction and is updated by
        // the write tx on commit, thread safety is inherited from the voron transaction
        private DocumentTransactionCache _documentsMetadataCache = new DocumentTransactionCache();

        public void Dispose()
        {
            var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(DocumentsStorage)}");

            exceptionAggregator.Execute(() =>
            {
                ContextPool?.Dispose();
            });

            exceptionAggregator.Execute(() =>
            {
                Environment?.Dispose();
            });

            exceptionAggregator.ThrowIfNeeded();
        }

        public void Initialize(bool generateNewDatabaseId = false)
        {
            if (_logger.IsInfoEnabled)
            {
                _logger.Info
                ("Starting to open document storage for " + (DocumentDatabase.Configuration.Core.RunInMemory
                     ? "<memory>"
                     : DocumentDatabase.Configuration.Core.DataDirectory.FullPath));
            }

            var options = GetStorageEnvironmentOptionsFromConfiguration(DocumentDatabase.Configuration, DocumentDatabase.IoChanges, DocumentDatabase.CatastrophicFailureNotification);

            options.OnNonDurableFileSystemError += DocumentDatabase.HandleNonDurableFileSystemError;
            options.OnRecoveryError += DocumentDatabase.HandleOnDatabaseRecoveryError;
            options.OnIntegrityErrorOfAlreadySyncedData += DocumentDatabase.HandleOnDatabaseIntegrityErrorOfAlreadySyncedData;

            options.GenerateNewDatabaseId = generateNewDatabaseId;
            options.CompressTxAboveSizeInBytes = DocumentDatabase.Configuration.Storage.CompressTxAboveSize.GetValue(SizeUnit.Bytes);
            options.ForceUsing32BitsPager = DocumentDatabase.Configuration.Storage.ForceUsing32BitsPager;
            options.EnablePrefetching = DocumentDatabase.Configuration.Storage.EnablePrefetching;
            options.TimeToSyncAfterFlushInSec = (int)DocumentDatabase.Configuration.Storage.TimeToSyncAfterFlush.AsTimeSpan.TotalSeconds;
            options.NumOfConcurrentSyncsPerPhysDrive = DocumentDatabase.Configuration.Storage.NumberOfConcurrentSyncsPerPhysicalDrive;
            options.AddToInitLog = _addToInitLog;
            options.Encryption.MasterKey = DocumentDatabase.MasterKey?.ToArray();
            options.Encryption.RegisterForJournalCompressionHandler();
            options.DoNotConsiderMemoryLockFailureAsCatastrophicError = DocumentDatabase.Configuration.Security.DoNotConsiderMemoryLockFailureAsCatastrophicError;
            if (DocumentDatabase.Configuration.Storage.MaxScratchBufferSize.HasValue)
                options.MaxScratchBufferSize = DocumentDatabase.Configuration.Storage.MaxScratchBufferSize.Value.GetValue(SizeUnit.Bytes);
            options.PrefetchSegmentSize = DocumentDatabase.Configuration.Storage.PrefetchBatchSize.GetValue(SizeUnit.Bytes);
            options.PrefetchResetThreshold = DocumentDatabase.Configuration.Storage.PrefetchResetThreshold.GetValue(SizeUnit.Bytes);
            options.SyncJournalsCountThreshold = DocumentDatabase.Configuration.Storage.SyncJournalsCountThreshold;
            options.IgnoreInvalidJournalErrors = DocumentDatabase.Configuration.Storage.IgnoreInvalidJournalErrors;
            options.SkipChecksumValidationOnDatabaseLoading = DocumentDatabase.Configuration.Storage.SkipChecksumValidationOnDatabaseLoading;
            options.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions = DocumentDatabase.Configuration.Storage.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions;

            try
            {
                Initialize(options);
            }
            catch (Exception)
            {
                options.Dispose();

                throw;
            }
        }

        public static StorageEnvironmentOptions GetStorageEnvironmentOptionsFromConfiguration(RavenConfiguration config, IoChangesNotifications ioChanges, CatastrophicFailureNotification catastrophicFailureNotification)
        {
            if (config.Core.RunInMemory)
                return StorageEnvironmentOptions.CreateMemoryOnly(
                    config.Core.DataDirectory.FullPath,
                    config.Storage.TempPath?.FullPath,
                    ioChanges,
                    catastrophicFailureNotification);

            return StorageEnvironmentOptions.ForPath(
                config.Core.DataDirectory.FullPath,
                config.Storage.TempPath?.FullPath,
                null,
                ioChanges,
                catastrophicFailureNotification
            );
        }

        public void Initialize(StorageEnvironmentOptions options)
        {
            options.SchemaVersion = SchemaUpgrader.CurrentVersion.DocumentsVersion;
            options.SchemaUpgrader = SchemaUpgrader.Upgrader(SchemaUpgrader.StorageType.Documents, null, this, null);
            try
            {
                DirectoryExecUtils.SubscribeToOnDirectoryInitializeExec(options, DocumentDatabase.Configuration.Storage, DocumentDatabase.Name, DirectoryExecUtils.EnvironmentType.Database, _logger);

                ContextPool = new DocumentsContextPool(DocumentDatabase);
                Environment = StorageLoader.OpenEnvironment(options, StorageEnvironmentWithType.StorageEnvironmentType.Documents);

                Environment.NewTransactionCreated += SetTransactionCache;
                Environment.AfterCommitWhenNewReadTransactionsPrevented += UpdateDocumentTransactionCache;

                using (var tx = Environment.WriteTransaction())
                {
                    NewPageAllocator.MaybePrefetchSections(
                        tx.LowLevelTransaction.RootObjects,
                        tx.LowLevelTransaction);

                    tx.CreateTree(TableSchema.CompressionDictionariesSlice);
                    tx.CreateTree(DocsSlice);
                    tx.CreateTree(LastReplicatedEtagsSlice);
                    tx.CreateTree(GlobalTreeSlice);

                    CollectionsSchema.Create(tx, CollectionsSlice, 32);

                    RevisionsStorage = new RevisionsStorage(DocumentDatabase, tx);
                    ExpirationStorage = new ExpirationStorage(DocumentDatabase, tx);
                    ConflictsStorage = new ConflictsStorage(DocumentDatabase, tx);
                    AttachmentsStorage = new AttachmentsStorage(DocumentDatabase, tx);
                    CountersStorage = new CountersStorage(DocumentDatabase, tx);
                    TimeSeriesStorage = new TimeSeriesStorage(DocumentDatabase, tx);

                    DocumentPut = new DocumentPutAction(this, DocumentDatabase);

                    InitializeLastEtag(tx);
                    _collectionsCache = ReadCollections(tx);

                    var cv = GetDatabaseChangeVector(tx);
                    var lastEtagInChangeVector = ChangeVectorUtils.GetEtagById(cv, DocumentDatabase.DbBase64Id);
                    _lastEtag = Math.Max(_lastEtag, lastEtagInChangeVector);

                    tx.Commit();
                }

                using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var cv = GetDatabaseChangeVector(context);
                    var lastEtagInChangeVector = ChangeVectorUtils.GetEtagById(cv, DocumentDatabase.DbBase64Id);
                    _lastEtag = Math.Max(_lastEtag, lastEtagInChangeVector);
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Could not open documents store for '{_name}' ({options}).", e);

                Dispose();
                options.Dispose();
                throw;
            }
        }

        private void SetTransactionCache(LowLevelTransaction tx)
        {
            tx.ImmutableExternalState = _documentsMetadataCache;

            if (tx.Flags != TransactionFlags.ReadWrite)
                return;

            tx.LastChanceToReadFromWriteTransactionBeforeCommit += ComputeTransactionCache_BeforeCommit;
        }

        private void ComputeTransactionCache_BeforeCommit(LowLevelTransaction llt)
        {
            var tx = llt.Transaction;
            if (tx == null)
                return;

            var currentCache = new DocumentTransactionCache
            {
                LastDocumentEtag = ReadLastDocumentEtag(tx),
                LastAttachmentsEtag = ReadLastAttachmentsEtag(tx),
                LastConflictEtag = ReadLastConflictsEtag(tx),
                LastCounterEtag = ReadLastCountersEtag(tx),
                LastTimeSeriesEtag = ReadLastTimeSeriesEtag(tx),
                LastEtag = ReadLastEtag(tx),
                LastRevisionsEtag = ReadLastRevisionsEtag(tx),
                LastTombstoneEtag = ReadLastTombstoneEtag(tx),
            };

            using (ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
            {
                Table.TableValueHolder holder = default;
                foreach (var collection in IterateCollectionNames(tx, ctx))
                {
                    var collectionName = new CollectionName(collection);
                    if (ReadLastDocument(tx, collectionName, CollectionTableType.Documents, ref holder) == false)
                        continue;

                    var colCache = new DocumentTransactionCache.CollectionCache
                    {
                        LastDocumentEtag = TableValueToEtag((int)DocumentsTable.Etag, ref holder.Reader),
                        LastChangeVector = TableValueToChangeVector(ctx, (int)DocumentsTable.ChangeVector, ref holder.Reader),
                    };

                    if (ReadLastDocument(tx, collectionName, CollectionTableType.Tombstones, ref holder))
                    {
                        colCache.LastTombstoneEtag = TableValueToEtag((int)DocumentsTable.Etag, ref holder.Reader);
                    }
                    currentCache.LastEtagsByCollection[collection] = colCache;
                }
            }
            // we set it on the current transaction because we aren't committed yet
            // we'll publish this after the commit finalization
            tx.LowLevelTransaction.ImmutableExternalState = currentCache;
        }

        private void UpdateDocumentTransactionCache(LowLevelTransaction obj)
        {
            _documentsMetadataCache = obj.ImmutableExternalState as DocumentTransactionCache;
        }

        public void AssertFixedSizeTrees(Transaction tx)
        {
            // here we validate the table fixed size indexes
            tx.OpenTable(DocsSchema, DocsSlice).AssertValidFixedSizeTrees();
            tx.OpenTable(DocsSchema, LastReplicatedEtagsSlice).AssertValidFixedSizeTrees();
            tx.OpenTable(DocsSchema, GlobalTreeSlice).AssertValidFixedSizeTrees();
        }

        private static void AssertTransaction(DocumentsOperationContext context)
        {
            if (context.Transaction == null) //precaution
                throw new InvalidOperationException("No active transaction found in the context, and at least read transaction is needed");
        }

        public static string GetDatabaseChangeVector(DocumentsOperationContext context)
        {
            return GetDatabaseChangeVector(context.Transaction.InnerTransaction);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string GetDatabaseChangeVector(Transaction tx)
        {
            if (tx == null)
                throw new InvalidOperationException("No active transaction found in the context, and at least read transaction is needed");
            var tree = tx.ReadTree(GlobalTreeSlice);
            var val = tree.Read(GlobalChangeVectorSlice);
            if (val == null)
            {
                return string.Empty;
            }
            return Encodings.Utf8.GetString(val.Reader.Base, val.Reader.Length);
        }

        internal HashSet<string> UnusedDatabaseIds;

        public bool HasUnusedDatabaseIds()
        {
            var list = UnusedDatabaseIds;
            if (list == null || list.Count == 0)
                return false;

            return true;
        }

        public bool TryRemoveUnusedIds(ref string changeVector)
        {
            if (string.IsNullOrEmpty(changeVector))
                return false;

            var list = UnusedDatabaseIds;
            if (list == null || list.Count == 0)
                return false;

            var parsed = changeVector.ToChangeVectorList();
            var removed = false;
            for (var i = parsed.Count - 1; i >= 0; i--)
            {
                var entry = parsed[i];
                if (list.Contains(entry.DbId))
                {
                    parsed.RemoveAt(i);
                    removed = true;
                }
            }

            if (removed == false)
                return false;

            changeVector = parsed.SerializeVector();
            return true;
        }

        public string CreateNextDatabaseChangeVector(DocumentsOperationContext context, string changeVector)
        {
            var databaseChangeVector = context.LastDatabaseChangeVector ?? GetDatabaseChangeVector(context);
            context.SkipChangeVectorValidation = TryRemoveUnusedIds(ref databaseChangeVector);
            changeVector = ChangeVectorUtils.MergeVectors(databaseChangeVector, changeVector);
            return ChangeVectorUtils.TryUpdateChangeVector(DocumentDatabase, changeVector).ChangeVector;
        }

        public (string ChangeVector, long Etag) GetNewChangeVector(DocumentsOperationContext context)
        {
            var etag = GenerateNextEtag();
            return (GetNewChangeVector(context, etag), etag);
        }

        public string GetNewChangeVector(DocumentsOperationContext context, long newEtag)
        {
            var changeVector = context.LastDatabaseChangeVector ??
                               (context.LastDatabaseChangeVector = GetDatabaseChangeVector(context));

            context.SkipChangeVectorValidation = TryRemoveUnusedIds(ref changeVector);

            if (string.IsNullOrEmpty(changeVector))
            {
                context.LastDatabaseChangeVector = ChangeVectorUtils.NewChangeVector(DocumentDatabase, newEtag);
                return context.LastDatabaseChangeVector;
            }

            var result = ChangeVectorUtils.TryUpdateChangeVector(DocumentDatabase, changeVector, newEtag);
            if (result.IsValid)
            {
                context.LastDatabaseChangeVector = result.ChangeVector;
            }

            return context.LastDatabaseChangeVector;
        }

        public void SetDatabaseChangeVector(DocumentsOperationContext context, string changeVector)
        {
            if (TryRemoveUnusedIds(ref changeVector) == false)
                ThrowOnNotUpdatedChangeVector(context, changeVector);

            var tree = context.Transaction.InnerTransaction.ReadTree(GlobalTreeSlice);
            using (Slice.From(context.Allocator, changeVector, out var slice))
            {
                tree.Add(GlobalChangeVectorSlice, slice);
            }
        }

        [Conditional("DEBUG")]
        private static void ThrowOnNotUpdatedChangeVector(DocumentsOperationContext context, string changeVector)
        {
            if (context.SkipChangeVectorValidation)
                return;

            var globalChangeVector = GetDatabaseChangeVector(context);

            if (globalChangeVector != changeVector &&
                globalChangeVector != null &&
                globalChangeVector.ToChangeVector().OrderByDescending(x => x).SequenceEqual(changeVector.ToChangeVector().OrderByDescending(x => x)) == false &&
                ChangeVectorUtils.GetConflictStatus(changeVector, globalChangeVector) != ConflictStatus.Update)
            {
                throw new InvalidOperationException($"Global Change Vector wasn't updated correctly. " +
                                                    $"Conflict status: {ChangeVectorUtils.GetConflictStatus(changeVector, globalChangeVector)}, " + System.Environment.NewLine +
                                                    $"Current global Change Vector: {globalChangeVector}, New Change Vector: {changeVector}");
            }
        }

        public static long ReadLastDocumentEtag(Transaction tx)
        {
            if (tx.IsWriteTransaction == false)
            {
                if (tx.LowLevelTransaction.ImmutableExternalState is DocumentTransactionCache cache)
                {
                    return cache.LastDocumentEtag;
                }
            }

            return ReadLastEtagFrom(tx, AllDocsEtagsSlice);
        }

        public static long ReadLastTombstoneEtag(Transaction tx)
        {
            if (tx.IsWriteTransaction == false)
            {
                if (tx.LowLevelTransaction.ImmutableExternalState is DocumentTransactionCache cache)
                {
                    return cache.LastTombstoneEtag;
                }
            }
            return ReadLastEtagFrom(tx, AllTombstonesEtagsSlice);
        }

        public static long ReadLastConflictsEtag(Transaction tx)
        {
            if (tx.IsWriteTransaction == false)
            {
                if (tx.LowLevelTransaction.ImmutableExternalState is DocumentTransactionCache cache)
                {
                    return cache.LastConflictEtag;
                }
            }
            return ReadLastEtagFrom(tx, ConflictsStorage.AllConflictedDocsEtagsSlice);
        }

        public static long ReadLastRevisionsEtag(Transaction tx)
        {
            if (tx.IsWriteTransaction == false)
            {
                if (tx.LowLevelTransaction.ImmutableExternalState is DocumentTransactionCache cache)
                {
                    return cache.LastRevisionsEtag;
                }
            }
            return ReadLastEtagFrom(tx, RevisionsStorage.AllRevisionsEtagsSlice);
        }

        public static long ReadLastAttachmentsEtag(Transaction tx)
        {
            if (tx.IsWriteTransaction == false)
            {
                if (tx.LowLevelTransaction.ImmutableExternalState is DocumentTransactionCache cache)
                {
                    return cache.LastAttachmentsEtag;
                }
            }
            return AttachmentsStorage.ReadLastEtag(tx);
        }

        public static long ReadLastCountersEtag(Transaction tx)
        {
            if (tx.IsWriteTransaction == false)
            {
                if (tx.LowLevelTransaction.ImmutableExternalState is DocumentTransactionCache cache)
                {
                    return cache.LastCounterEtag;
                }
            }
            return ReadLastEtagFrom(tx, CountersStorage.AllCountersEtagSlice);
        }

        public static long ReadLastTimeSeriesEtag(Transaction tx)
        {
            if (tx.IsWriteTransaction == false)
            {
                if (tx.LowLevelTransaction.ImmutableExternalState is DocumentTransactionCache cache)
                {
                    return cache.LastTimeSeriesEtag;
                }
            }
            return ReadLastEtagFrom(tx, TimeSeriesStorage.AllTimeSeriesEtagSlice);
        }

        private static long ReadLastEtagFrom(Transaction tx, Slice name)
        {
            using (var fst = new FixedSizeTree(tx.LowLevelTransaction,
                tx.LowLevelTransaction.RootObjects,
                name, sizeof(long),
                clone: false))
            {
                using (var it = fst.Iterate())
                {
                    if (it.SeekToLast())
                        return it.CurrentKey;
                }
            }

            return 0;
        }

        public static long ReadLastEtag(Transaction tx)
        {
            if (tx.IsWriteTransaction == false)
            {
                if (tx.LowLevelTransaction.ImmutableExternalState is DocumentTransactionCache cache)
                {
                    return cache.LastEtag;
                }
            }

            var tree = tx.CreateTree(EtagsSlice);
            var readResult = tree.Read(LastEtagSlice);
            long lastEtag = 0;
            if (readResult != null)
                lastEtag = readResult.Reader.ReadLittleEndianInt64();

            var lastDocumentEtag = ReadLastDocumentEtag(tx);
            if (lastDocumentEtag > lastEtag)
                lastEtag = lastDocumentEtag;

            var lastTombstoneEtag = ReadLastTombstoneEtag(tx);
            if (lastTombstoneEtag > lastEtag)
                lastEtag = lastTombstoneEtag;

            var lastConflictEtag = ReadLastConflictsEtag(tx);
            if (lastConflictEtag > lastEtag)
                lastEtag = lastConflictEtag;

            var lastRevisionsEtag = ReadLastRevisionsEtag(tx);
            if (lastRevisionsEtag > lastEtag)
                lastEtag = lastRevisionsEtag;

            var lastAttachmentEtag = ReadLastAttachmentsEtag(tx);
            if (lastAttachmentEtag > lastEtag)
                lastEtag = lastAttachmentEtag;

            var lastCounterEtag = ReadLastCountersEtag(tx);
            if (lastCounterEtag > lastEtag)
                lastEtag = lastCounterEtag;

            var lastTimeSeriesEtag = ReadLastTimeSeriesEtag(tx);
            if (lastTimeSeriesEtag > lastEtag)
                lastEtag = lastTimeSeriesEtag;

            return lastEtag;
        }

        public IEnumerable<Document> GetDocumentsStartingWith(DocumentsOperationContext context, string idPrefix, string matches, string exclude, string startAfterId,
            long start, long take, string collection, DocumentFields fields = DocumentFields.All)
        {
            var isAllDocs = collection == Constants.Documents.Collections.AllDocumentsCollection;
            var requestedDataField = fields.HasFlag(DocumentFields.Data);
            if (isAllDocs == false && requestedDataField == false)
                fields |= DocumentFields.Data;

            foreach (var doc in GetDocumentsStartingWith(context, idPrefix, matches, exclude, startAfterId, start, take, fields: fields))
            {
                if (isAllDocs)
                {
                    yield return doc;
                    continue;
                }

                if (IsMatch(doc, collection) == false)
                {
                    doc.Dispose();
                    continue;
                }

                if (requestedDataField == false)
                {
                    doc.Data.Dispose();
                    doc.Data = null;
                }

                yield return doc;
            }

            static bool IsMatch(Document doc, string collection)
            {
                if (doc.TryGetMetadata(out var metadata) == false)
                    return false;

                if (metadata.TryGet(Constants.Documents.Metadata.Collection, out string c) == false)
                    return false;

                if (string.Equals(c, collection, StringComparison.OrdinalIgnoreCase) == false)
                    return false;

                return true;
            }
        }

        public IEnumerable<Document> GetDocumentsStartingWith(DocumentsOperationContext context, string idPrefix, string matches, string exclude, string startAfterId,
            long start, long take, Reference<long> skip = null, DocumentFields fields = DocumentFields.All)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            var isStartAfter = string.IsNullOrWhiteSpace(startAfterId) == false;
            var needsWildcardMatch = string.IsNullOrEmpty(matches) == false || string.IsNullOrEmpty(exclude) == false;

            var startAfterSlice = Slices.Empty;
            using (DocumentIdWorker.GetSliceFromId(context, idPrefix, out Slice prefixSlice))
            using (isStartAfter ? (IDisposable)DocumentIdWorker.GetSliceFromId(context, startAfterId, out startAfterSlice) : null)
            {
                foreach (var result in table.SeekByPrimaryKeyPrefix(prefixSlice, startAfterSlice, skip?.Value ?? 0))
                {
                    var document = TableValueToDocument(context, ref result.Value.Reader, fields);
                    string documentId = document.Id;
                    if (documentId.StartsWith(idPrefix, StringComparison.OrdinalIgnoreCase) == false)
                    {
                        document.Dispose();
                        break;
                    }

                    if (needsWildcardMatch)
                    {
                        var idTest = documentId.Substring(idPrefix.Length);
                        if (WildcardMatcher.Matches(matches, idTest) == false || WildcardMatcher.MatchesExclusion(exclude, idTest))
                        {
                            if (skip != null)
                                skip.Value++;

                            document.Dispose();
                            continue;
                        }
                    }

                    if (start > 0)
                    {
                        if (skip != null)
                            skip.Value++;

                        start--;
                        document.Dispose();
                        continue;
                    }

                    if (take-- <= 0)
                    {
                        document.Dispose();
                        yield break;
                    }

                    yield return document;
                }
            }
        }

        public IEnumerable<Document> GetDocumentsInReverseEtagOrder(DocumentsOperationContext context, long start, long take)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekBackwardFromLast(DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice], start))
            {
                if (take-- <= 0)
                    yield break;
                yield return TableValueToDocument(context, ref result.Reader);
            }
        }

        public IEnumerable<Document> GetDocumentsInReverseEtagOrderFrom(DocumentsOperationContext context, long etag, long take, long skip)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekBackwardFrom(DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice], etag, skip))
            {
                if (take-- <= 0)
                    yield break;
                yield return TableValueToDocument(context, ref result.Reader);
            }
        }

        public IEnumerable<Document> GetDocumentsInReverseEtagOrder(DocumentsOperationContext context, string collection, long start, long take)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                yield break;

            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema,
                collectionName.GetTableName(CollectionTableType.Documents));

            if (table == null)
                yield break;

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekBackwardFromLast(DocsSchema.FixedSizeIndexes[CollectionEtagsSlice], start))
            {
                if (take-- <= 0)
                    yield break;
                yield return TableValueToDocument(context, ref result.Reader);
            }
        }

        private TestingStuff _forTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (_forTestingPurposes != null)
                return _forTestingPurposes;

            return _forTestingPurposes = new TestingStuff();
        }

        internal class TestingStuff
        {
            public ManualResetEventSlim DelayDocumentLoad;
        }

        public IEnumerable<Document> GetDocumentsFrom(DocumentsOperationContext context, long etag, long start, long take, DocumentFields fields = DocumentFields.All)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice], etag, start))
            {
                if (take-- <= 0)
                {
                    yield break;
                }

                yield return TableValueToDocument(context, ref result.Reader, fields);
            }
        }

        public IEnumerable<DocumentReplicationItem> GetDocumentsFrom(DocumentsOperationContext context, long etag)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice], etag, 0))
            {
                yield return DocumentReplicationItem.From(TableValueToDocument(context, ref result.Reader));
            }
        }

        public IEnumerable<Document> GetDocuments(DocumentsOperationContext context, List<Slice> ids, long start, long take, Reference<int> totalCount)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            foreach (var id in ids)
            {
                // id must be lowercased
                if (table.ReadByKey(id, out TableValueReader reader) == false)
                    continue;

                totalCount.Value++;

                if (start > 0)
                {
                    start--;
                    continue;
                }
                if (take-- <= 0)
                    continue; // we need to calculate totalCount correctly

                yield return TableValueToDocument(context, ref reader);
            }
        }

        public IEnumerable<Document> GetDocuments(DocumentsOperationContext context, IEnumerable<string> ids, long start, long take, Reference<int> totalCount)
        {
            var listOfIds = new List<Slice>();
            foreach (var id in ids)
            {
                Slice.From(context.Allocator, id.ToLowerInvariant(), out Slice slice);
                listOfIds.Add(slice);
            }

            return GetDocuments(context, listOfIds, start, take, totalCount);
        }

        public IEnumerable<Document> GetDocuments(DocumentsOperationContext context, List<Slice> ids, string collection, long start, long take, Reference<int> totalCount)
        {
            foreach (var doc in GetDocuments(context, ids, start, take, totalCount))
            {
                if (collection == Client.Constants.Documents.Collections.AllDocumentsCollection)
                {
                    yield return doc;
                    continue;
                }

                if (doc.TryGetMetadata(out var metadata) == false)
                {
                    totalCount.Value--;
                    continue;
                }
                if (metadata.TryGet(Client.Constants.Documents.Metadata.Collection, out string c) == false)
                {
                    totalCount.Value--;
                    continue;
                }
                if (string.Equals(c, collection, StringComparison.OrdinalIgnoreCase) == false)
                {
                    totalCount.Value--;
                    continue;
                }

                yield return doc;
            }
        }

        public IEnumerable<Document> GetDocumentsFrom(DocumentsOperationContext context, string collection, long etag, long start, long take, DocumentFields fields = DocumentFields.All)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                yield break;

            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema,
                collectionName.GetTableName(CollectionTableType.Documents));

            if (table == null)
                yield break;

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[CollectionEtagsSlice], etag, start))
            {
                if (take-- <= 0)
                    yield break;

                _forTestingPurposes?.DelayDocumentLoad?.Wait(DocumentDatabase.DatabaseShutdown);

                yield return TableValueToDocument(context, ref result.Reader, fields);
            }
        }

        public IEnumerable<Document> GetDocumentsFrom(DocumentsOperationContext context, List<string> collections, long etag, long take)
        {
            foreach (var collection in collections)
            {
                if (take <= 0)
                    yield break;

                foreach (var document in GetDocumentsFrom(context, collection, etag, 0, long.MaxValue))
                {
                    if (take-- <= 0)
                        yield break;

                    yield return document;
                }
            }
        }

        public DocumentOrTombstone GetDocumentOrTombstone(DocumentsOperationContext context, string id, bool throwOnConflict = true)
        {
            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("Argument is null or whitespace", nameof(id));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Put", nameof(context));

            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
            {
                return GetDocumentOrTombstone(context, lowerId, throwOnConflict);
            }
        }

        public struct DocumentOrTombstone
        {
            public Document Document;
            public Tombstone Tombstone;
            public bool Missing => Document == null && Tombstone == null;
        }

        public DocumentOrTombstone GetDocumentOrTombstone(DocumentsOperationContext context, Slice lowerId, bool throwOnConflict = true)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                return default(DocumentOrTombstone); // never hit
            }

            try
            {
                var doc = Get(context, lowerId);
                if (doc != null)
                    return new DocumentOrTombstone { Document = doc };
            }
            catch (DocumentConflictException)
            {
                if (throwOnConflict)
                    throw;
                return new DocumentOrTombstone();
            }

            var tombstoneTable = new Table(TombstonesSchema, context.Transaction.InnerTransaction);

            // return tombstone in any collection with the requested id
            foreach (var (tombstoneKey, tvh) in tombstoneTable.SeekByPrimaryKeyPrefix(lowerId, Slices.Empty, 0))
            {
                if (IsTombstoneOfId(tombstoneKey, lowerId))
                {
                    return new DocumentOrTombstone
                    {
                        Tombstone = TableValueToTombstone(context, ref tvh.Reader)
                    };
                }
                break;
            }

            return new DocumentOrTombstone
            {
                Tombstone = null
            };
        }

        public Document Get(DocumentsOperationContext context, string id, DocumentFields fields = DocumentFields.All, bool throwOnConflict = true)
        {
            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("Argument is null or whitespace", nameof(id));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Get", nameof(context));

            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
            {
                return Get(context, lowerId, fields, throwOnConflict);
            }
        }

        public Document Get(DocumentsOperationContext context, Slice lowerId, DocumentFields fields = DocumentFields.All, bool throwOnConflict = true, bool skipValidationInDebug = false)
        {
            if (GetTableValueReaderForDocument(context, lowerId, throwOnConflict, out TableValueReader tvr) == false)
                return null;

            var doc = TableValueToDocument(context, ref tvr, fields, skipValidationInDebug);

            context.DocumentDatabase.HugeDocuments.AddIfDocIsHuge(doc);

            return doc;
        }

        public Document GetByEtag(DocumentsOperationContext context, long etag)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);
            var index = DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice];

            if (table.Read(context.Allocator, index, etag, out var tvr) == false)
                return null;

            return TableValueToDocument(context, ref tvr);
        }

        public Tombstone GetTombstoneByEtag(DocumentsOperationContext context, long etag)
        {
            var table = new Table(TombstonesSchema, context.Transaction.InnerTransaction);
            var index = TombstonesSchema.FixedSizeIndexes[AllTombstonesEtagsSlice];

            if (table.Read(context.Allocator, index, etag, out var tvr) == false)
                return null;

            return TableValueToTombstone(context, ref tvr);
        }

        public long GetNumberOfTombstones(DocumentsOperationContext context)
        {
            var fstIndex = TombstonesSchema.FixedSizeIndexes[AllTombstonesEtagsSlice];
            var fst = context.Transaction.InnerTransaction.FixedTreeFor(fstIndex.Name, sizeof(long));
            return fst.NumberOfEntries;
        }

        public IEnumerable<LazyStringValue> GetAllIds(DocumentsOperationContext context)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice], 0, 0))
            {
                yield return TableValueToId(context, (int)DocumentsTable.Id, ref result.Reader);
            }
        }

        public (int ActualSize, int AllocatedSize)? GetDocumentMetrics(DocumentsOperationContext context, string id)
        {
            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
            {
                var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

                if (table.ReadByKey(lowerId, out var tvr) == false)
                {
                    return null;
                }
                var allocated = table.GetAllocatedSize(tvr.Id);

                return (tvr.Size, allocated);
            }
        }

        public bool GetTableValueReaderForDocument(DocumentsOperationContext context, Slice lowerId, bool throwOnConflict, out TableValueReader tvr)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            if (table.ReadByKey(lowerId, out tvr) == false)
            {
                if (throwOnConflict && ConflictsStorage.ConflictsCount > 0)
                    ConflictsStorage.ThrowOnDocumentConflict(context, lowerId);

                return false;
            }
            return true;
        }

        public bool HasMoreOfTombstonesAfter(
            DocumentsOperationContext context,
            long etag,
            int maxAllowed)
        {
            var table = new Table(TombstonesSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var _ in table.SeekForwardFrom(TombstonesSchema.FixedSizeIndexes[AllTombstonesEtagsSlice], etag, 0))
            {
                if (maxAllowed-- < 0)
                    return true;
            }
            return false;
        }

        public IEnumerable<Tombstone> GetTombstonesFrom(DocumentsOperationContext context, long etag, long start, long take)
        {
            var table = new Table(TombstonesSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(TombstonesSchema.FixedSizeIndexes[AllTombstonesEtagsSlice], etag, start))
            {
                if (take-- <= 0)
                    yield break;

                yield return TableValueToTombstone(context, ref result.Reader);
            }
        }

        public IEnumerable<Tombstone> GetTombstonesInReverseEtagOrderFrom(DocumentsOperationContext context, long etag, long start, long take)
        {
            var table = new Table(TombstonesSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekBackwardFrom(TombstonesSchema.FixedSizeIndexes[AllTombstonesEtagsSlice], etag, start))
            {
                if (take-- <= 0)
                    yield break;

                yield return TableValueToTombstone(context, ref result.Reader);
            }
        }

        public Tombstone GetTombstoneAtOrBefore(DocumentsOperationContext context, long etag)
        {
            return GetTombstonesInReverseEtagOrderFrom(context, etag, 0, 2).FirstOrDefault(t => t.Etag <= etag);
        }

        public IEnumerable<Tombstone> GetTombstonesFrom(DocumentsOperationContext context, List<string> collections, long etag, long take)
        {
            foreach (var collection in collections)
            {
                if (take <= 0)
                    yield break;

                foreach (var tombstone in GetTombstonesFrom(context, collection, etag, 0, long.MaxValue))
                {
                    if (take-- <= 0)
                        yield break;

                    yield return tombstone;
                }
            }
        }

        public IEnumerable<ReplicationBatchItem> GetTombstonesFrom(DocumentsOperationContext context, long etag)
        {
            var table = new Table(TombstonesSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(TombstonesSchema.FixedSizeIndexes[AllTombstonesEtagsSlice], etag, 0))
            {
                yield return TombstoneReplicationItem.From(context, TableValueToTombstone(context, ref result.Reader));
            }
        }

        public IEnumerable<Tombstone> GetAttachmentTombstonesFrom(
            DocumentsOperationContext context,
            long etag,
            long start,
            long take)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, AttachmentsStorage.AttachmentsTombstones);

            if (table == null)
                yield break;

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(TombstonesSchema.FixedSizeIndexes[CollectionEtagsSlice], etag, start))
            {
                if (take-- <= 0)
                    yield break;

                yield return TableValueToTombstone(context, ref result.Reader);
            }
        }

        public IEnumerable<Tombstone> GetTombstonesFrom(
            DocumentsOperationContext context,
            string collection,
            long etag,
            long start,
            long take)
        {
            string tableName;

            if (collection == AttachmentsStorage.AttachmentsTombstones ||
                collection == RevisionsStorage.RevisionsTombstones)
            {
                tableName = collection;
            }
            else
            {
                var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
                if (collectionName == null)
                    yield break;

                tableName = collectionName.GetTableName(CollectionTableType.Tombstones);
            }

            var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, tableName);

            if (table == null)
                yield break;

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(TombstonesSchema.FixedSizeIndexes[CollectionEtagsSlice], etag, start))
            {
                if (take-- <= 0)
                    yield break;

                yield return TableValueToTombstone(context, ref result.Reader);
            }
        }

        public long GetLastDocumentEtag(Transaction tx, string collection)
        {
            if (tx.IsWriteTransaction == false)
            {
                if (tx.LowLevelTransaction.ImmutableExternalState is DocumentTransactionCache cache)
                {
                    if (cache.LastEtagsByCollection.TryGetValue(collection, out var col))
                        return col.LastDocumentEtag;
                }
            }
            Table.TableValueHolder result = null;
            if (LastDocument(tx, collection, ref result) == false)
                return 0;

            return TableValueToEtag((int)DocumentsTable.Etag, ref result.Reader);
        }

        public string GetLastDocumentChangeVector(Transaction tx, JsonOperationContext ctx, string collection)
        {
            if (tx.IsWriteTransaction == false)
            {
                if (tx.LowLevelTransaction.ImmutableExternalState is DocumentTransactionCache cache)
                {
                    if (cache.LastEtagsByCollection.TryGetValue(collection, out var col))
                        return col.LastChangeVector;
                }
            }
            Table.TableValueHolder result = null;
            if (LastDocument(tx, collection, ref result) == false)
                return null;

            return TableValueToChangeVector(ctx, (int)DocumentsTable.ChangeVector, ref result.Reader);
        }

        private bool LastDocument(Transaction transaction, string collection, ref Table.TableValueHolder result)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                return false;

            return ReadLastDocument(transaction, collectionName, CollectionTableType.Documents, ref result);
        }

        private static bool ReadLastDocument(Transaction transaction, CollectionName collectionName, CollectionTableType collectionType, ref Table.TableValueHolder result)
        {
            var table = transaction.OpenTable(DocsSchema,
                collectionName.GetTableName(collectionType)
            );

            // ReSharper disable once UseNullPropagation
            if (table == null)
                return false;

            result = table.ReadLast(DocsSchema.FixedSizeIndexes[CollectionEtagsSlice]);
            if (result == null)
                return false;

            return true;
        }

        public long GetLastTombstoneEtag(Transaction tx, string collection)
        {
            if (tx.IsWriteTransaction == false)
            {
                if (tx.LowLevelTransaction.ImmutableExternalState is DocumentTransactionCache cache)
                {
                    if (cache.LastEtagsByCollection.TryGetValue(collection, out var col))
                        return col.LastTombstoneEtag;
                }
            }

            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                return 0;

            var table = tx.OpenTable(TombstonesSchema,
                collectionName.GetTableName(CollectionTableType.Tombstones));

            // ReSharper disable once UseNullPropagation
            if (table == null)
                return 0;

            var result = table.ReadLast(TombstonesSchema.FixedSizeIndexes[CollectionEtagsSlice]);
            if (result == null)
                return 0;

            return TableValueToEtag(1, ref result.Reader);
        }

        public bool HasTombstonesWithEtagGreaterThanStartAndLowerThanOrEqualToEnd(DocumentsOperationContext context, string collection,
            long start,
            long end)
        {
            if (start >= end)
                return false;

            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                return false;

            var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema,
                collectionName.GetTableName(CollectionTableType.Tombstones));

            if (table == null)
                return false;

            return table.HasEntriesGreaterThanStartAndLowerThanOrEqualToEnd(TombstonesSchema.FixedSizeIndexes[CollectionEtagsSlice], start, end);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Document TableValueToDocument(DocumentsOperationContext context, ref TableValueReader tvr, DocumentFields fields = DocumentFields.All, bool skipValidationInDebug = false)
        {
            var document = ParseDocument(context, ref tvr, fields);
#if DEBUG
            if (skipValidationInDebug == false)
            {
                Transaction.DebugDisposeReaderAfterTransaction(context.Transaction.InnerTransaction, document.Data);
                DocumentPutAction.AssertMetadataWasFiltered(document.Data);
                AssertMetadataKey(document.Id, document.Data, document.Flags, DocumentFlags.HasAttachments, Constants.Documents.Metadata.Attachments);
                AssertMetadataKey(document.Id, document.Data, document.Flags, DocumentFlags.HasCounters, Constants.Documents.Metadata.Counters);
                AssertMetadataKey(document.Id, document.Data, document.Flags, DocumentFlags.HasTimeSeries, Constants.Documents.Metadata.TimeSeries);
            }
#endif
            return document;
        }

        [Conditional("DEBUG")]
        public void AssertMetadataKey(string id, BlittableJsonReaderObject document, DocumentFlags flags, DocumentFlags assertionFlag, string assertionKey)
        {
            if (document == null)
                return;

            if (flags.Contain(assertionFlag))
            {
                if (document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                    metadata.TryGet(assertionKey, out BlittableJsonReaderArray _) == false)
                {
                    Debug.Assert(false, $"Found {assertionFlag} flag but {assertionKey} is missing from metadata in document {id} (database: {DocumentDatabase.Name}).");
                }
            }
            else
            {
                if (document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) &&
                    metadata.TryGet(assertionKey, out BlittableJsonReaderArray values))
                {
                    Debug.Assert(false, $"Found {assertionKey}({values.Length}) in metadata but {flags} flag is missing in document {id} (database: {DocumentDatabase.Name}).");
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static Document ParseDocument(JsonOperationContext context, ref TableValueReader tvr, DocumentFields fields)
        {
            if (fields == DocumentFields.All)
            {
                return new Document
                {
                    StorageId = tvr.Id,
                    LowerId = TableValueToString(context, (int)DocumentsTable.LowerId, ref tvr),
                    Id = TableValueToId(context, (int)DocumentsTable.Id, ref tvr),
                    Etag = TableValueToEtag((int)DocumentsTable.Etag, ref tvr),
                    Data = new BlittableJsonReaderObject(tvr.Read((int)DocumentsTable.Data, out int size), size, context),
                    ChangeVector = TableValueToChangeVector(context, (int)DocumentsTable.ChangeVector, ref tvr),
                    LastModified = TableValueToDateTime((int)DocumentsTable.LastModified, ref tvr),
                    Flags = TableValueToFlags((int)DocumentsTable.Flags, ref tvr),
                    TransactionMarker = TableValueToShort((int)DocumentsTable.TransactionMarker, nameof(DocumentsTable.TransactionMarker), ref tvr),
                };
            }

            return ParseDocumentPartial(context, ref tvr, fields);
        }

        private static Document ParseDocumentPartial(JsonOperationContext context, ref TableValueReader tvr, DocumentFields fields)
        {
            var result = new Document();

            if (fields.Contain(DocumentFields.LowerId))
                result.LowerId = TableValueToString(context, (int)DocumentsTable.LowerId, ref tvr);

            if (fields.Contain(DocumentFields.Id))
                result.Id = TableValueToId(context, (int)DocumentsTable.Id, ref tvr);

            if (fields.Contain(DocumentFields.Data))
                result.Data = new BlittableJsonReaderObject(tvr.Read((int)DocumentsTable.Data, out int size), size, context);

            if (fields.Contain(DocumentFields.ChangeVector))
                result.ChangeVector = TableValueToChangeVector(context, (int)DocumentsTable.ChangeVector, ref tvr);

            result.Etag = TableValueToEtag((int)DocumentsTable.Etag, ref tvr);
            result.LastModified = TableValueToDateTime((int)DocumentsTable.LastModified, ref tvr);
            result.Flags = TableValueToFlags((int)DocumentsTable.Flags, ref tvr);
            result.StorageId = tvr.Id;
            result.TransactionMarker = TableValueToShort((int)DocumentsTable.TransactionMarker, nameof(DocumentsTable.TransactionMarker), ref tvr);

            return result;
        }

        public static Document ParseRawDataSectionDocumentWithValidation(JsonOperationContext context, ref TableValueReader tvr, int expectedSize)
        {
            tvr.Read((int)DocumentsTable.Data, out int size);
            if (size > expectedSize || size <= 0)
                throw new ArgumentException("Data size is invalid, possible corruption when parsing BlittableJsonReaderObject", nameof(size));

            return ParseDocument(context, ref tvr, DocumentFields.All);
        }

        public static Tombstone TableValueToTombstone(JsonOperationContext context, ref TableValueReader tvr)
        {
            if (tvr.Pointer == null)
                return null;

            var result = new Tombstone
            {
                StorageId = tvr.Id,
                LowerId = TableValueToString(context, (int)TombstoneTable.LowerId, ref tvr),
                Etag = TableValueToEtag((int)TombstoneTable.Etag, ref tvr),
                DeletedEtag = TableValueToEtag((int)TombstoneTable.DeletedEtag, ref tvr),
                Type = *(Tombstone.TombstoneType*)tvr.Read((int)TombstoneTable.Type, out int _),
                TransactionMarker = *(short*)tvr.Read((int)TombstoneTable.TransactionMarker, out int _),
                ChangeVector = TableValueToChangeVector(context, (int)TombstoneTable.ChangeVector, ref tvr)
            };

            if (result.Type == Tombstone.TombstoneType.Document)
            {
                result.Collection = TableValueToId(context, (int)TombstoneTable.Collection, ref tvr);
                result.Flags = TableValueToFlags((int)TombstoneTable.Flags, ref tvr);
                result.LastModified = TableValueToDateTime((int)TombstoneTable.LastModified, ref tvr);
                result.LowerId = UnwrapLowerIdIfNeeded(context, result.LowerId);
            }
            else if (result.Type == Tombstone.TombstoneType.Revision)
            {
                result.Collection = TableValueToId(context, (int)TombstoneTable.Collection, ref tvr);
            }

            return result;
        }

        public DeleteOperationResult? Delete(DocumentsOperationContext context, string id, string expectedChangeVector)
        {
            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
            using (var cv = context.GetLazyString(expectedChangeVector))
            {
                return Delete(context, lowerId, id, expectedChangeVector: cv);
            }
        }

        public DeleteOperationResult? Delete(DocumentsOperationContext context, Slice lowerId, string id,
            LazyStringValue expectedChangeVector, long? lastModifiedTicks = null, string changeVector = null,
            CollectionName collectionName = null, NonPersistentDocumentFlags nonPersistentFlags = NonPersistentDocumentFlags.None,
            DocumentFlags documentFlags = DocumentFlags.None)
        {
            if (ConflictsStorage.ConflictsCount != 0)
            {
                var result = ConflictsStorage.DeleteConflicts(context, lowerId, expectedChangeVector, changeVector);
                if (result != null)
                    return result;
            }

            var local = GetDocumentOrTombstone(context, lowerId, throwOnConflict: false);
            var modifiedTicks = GetOrCreateLastModifiedTicks(lastModifiedTicks);

            if (local.Tombstone != null)
            {
                if (expectedChangeVector != null)
                    throw new ConcurrencyException($"Document {local.Tombstone.LowerId} does not exist, but delete was called with change vector '{expectedChangeVector}'. " +
                                                   "Optimistic concurrency violation, transaction will be aborted.");
                if (collectionName == null)
                {
                    collectionName = ExtractCollectionName(context, local.Tombstone.Collection);
                }
                else if (local.Tombstone.Collection.Equals(collectionName.Name) == false)
                {
                    // ensure the table for the tombstones is created
                    ExtractCollectionName(context, collectionName.Name);
                }

                var tombstoneTable = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema,
                    collectionName.GetTableName(CollectionTableType.Tombstones));

                if (tombstoneTable.IsOwned(local.Tombstone.StorageId))
                {
                    tombstoneTable.Delete(local.Tombstone.StorageId);
                }

                var localFlags = local.Tombstone.Flags.Strip(DocumentFlags.FromClusterTransaction);
                var flags = localFlags | documentFlags;

                if (collectionName.IsHiLo == false &&
                    (flags & DocumentFlags.Artificial) != DocumentFlags.Artificial)
                {
                    var revisionsStorage = DocumentDatabase.DocumentsStorage.RevisionsStorage;
                    if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromReplication) == false &&
                        (revisionsStorage.Configuration != null || flags.Contain(DocumentFlags.Resolved)))
                    {
                        revisionsStorage.Delete(context, id, lowerId, collectionName, changeVector ?? local.Tombstone.ChangeVector,
                            modifiedTicks, nonPersistentFlags, documentFlags);
                    }
                }

                // we update the tombstone
                var etag = CreateTombstone(context,
                    lowerId,
                    local.Tombstone.Etag,
                    collectionName,
                    local.Tombstone.ChangeVector,
                    modifiedTicks,
                    changeVector,
                    flags,
                    nonPersistentFlags).Etag;

                EnsureLastEtagIsPersisted(context, etag);

                // We have to raise the notification here because even though we have deleted
                // a deleted value, we changed the change vector. And maybe we need to replicate
                // that. Another issue is that the last tombstone etag has changed, and we need
                // to let the indexes catch up to us here, even if they'll just do a noop.

                context.Transaction.AddAfterCommitNotification(new DocumentChange
                {
                    Type = DocumentChangeTypes.Delete,
                    Id = id,
                    ChangeVector = changeVector,
                    CollectionName = collectionName.Name,
                });

                return new DeleteOperationResult
                {
                    Collection = collectionName,
                    Etag = etag
                };
            }

            if (local.Document != null)
            {
                // just delete the document
                var doc = local.Document;
                if (expectedChangeVector != null && doc.ChangeVector.CompareTo(expectedChangeVector) != 0)
                {
                    throw new ConcurrencyException(
                        $"Document {id} has change vector {doc.ChangeVector}, but Delete was called with change vector '{expectedChangeVector}'. " +
                        "Optimistic concurrency violation, transaction will be aborted.")
                    {
                        ActualChangeVector = doc.ChangeVector,
                        ExpectedChangeVector = expectedChangeVector
                    };
                }

                collectionName = ExtractCollectionName(context, doc.Data);
                var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, collectionName.GetTableName(CollectionTableType.Documents));

                var ptr = table.DirectRead(doc.StorageId, out int size);
                var tvr = new TableValueReader(ptr, size);
                var flags = TableValueToFlags((int)DocumentsTable.Flags, ref tvr).Strip(DocumentFlags.FromClusterTransaction) | documentFlags;

                long etag;
                using (TableValueToSlice(context, (int)DocumentsTable.LowerId, ref tvr, out Slice tombstoneId))
                {
                    var tombstone = CreateTombstone(
                        context,
                        tombstoneId,
                        doc.Etag,
                        collectionName,
                        doc.ChangeVector,
                        modifiedTicks,
                        changeVector,
                        flags,
                        nonPersistentFlags);
                    changeVector = tombstone.ChangeVector;
                    etag = tombstone.Etag;
                }

                EnsureLastEtagIsPersisted(context, etag);

                var revisionsStorage = DocumentDatabase.DocumentsStorage.RevisionsStorage;

                if (collectionName.IsHiLo == false &&
                    ((flags & DocumentFlags.Artificial) != DocumentFlags.Artificial) &&
                    (nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromReplication) == false))
                {
                    var shouldVersion = DocumentDatabase.DocumentsStorage.RevisionsStorage.ShouldVersionDocument(
                        collectionName, nonPersistentFlags, local.Document.Data, null, context, id, lastModifiedTicks, ref flags, out var configuration);

                    if (shouldVersion)
                    {
                        if (DocumentDatabase.DocumentsStorage.RevisionsStorage.ShouldVersionOldDocument(flags, local.Document.Data))
                        {
                            DocumentDatabase.DocumentsStorage.RevisionsStorage.Put(context, id, local.Document.Data, flags | DocumentFlags.HasRevisions, NonPersistentDocumentFlags.None,
                                local.Document.ChangeVector, local.Document.LastModified.Ticks, configuration, collectionName);
                        }

                        flags |= DocumentFlags.HasRevisions;
                        revisionsStorage.Delete(context, id, lowerId, collectionName, changeVector ?? local.Tombstone.ChangeVector,
                            modifiedTicks, nonPersistentFlags, documentFlags);
                    }
                }

                if (((flags & DocumentFlags.HasRevisions) == DocumentFlags.HasRevisions) &&
                    (revisionsStorage.Configuration == null) &&
                    ((flags & DocumentFlags.Resolved) != DocumentFlags.Resolved))
                    revisionsStorage.DeleteRevisionsFor(context, id);

                table.Delete(doc.StorageId);

                if ((flags & DocumentFlags.HasAttachments) == DocumentFlags.HasAttachments)
                    AttachmentsStorage.DeleteAttachmentsOfDocument(context, lowerId, changeVector, modifiedTicks);

                CountersStorage.DeleteCountersForDocument(context, id, collectionName);
                TimeSeriesStorage.DeleteAllTimeSeriesForDocument(context, id, collectionName);

                context.Transaction.AddAfterCommitNotification(new DocumentChange
                {
                    Type = DocumentChangeTypes.Delete,
                    Id = id,
                    ChangeVector = changeVector,
                    CollectionName = collectionName.Name,
                });

                return new DeleteOperationResult
                {
                    Collection = collectionName,
                    Etag = etag
                };
            }
            else
            {
                // we adding a tombstone without having any previous document, it could happened if this was called
                // from the incoming replication or if we delete document that wasn't exist at the first place.
                if (expectedChangeVector != null)
                    throw new ConcurrencyException($"Document {lowerId} does not exist, but delete was called with change vector '{expectedChangeVector}'. " +
                                                   "Optimistic concurrency violation, transaction will be aborted.");

                if (collectionName == null)
                {
                    // this basically mean that we tried to delete document that doesn't exist.
                    return null;
                }

                // ensures that the collection trees will be created
                collectionName = ExtractCollectionName(context, collectionName.Name);

                var etag = CreateTombstone(context,
                    lowerId,
                    GenerateNextEtagForReplicatedTombstoneMissingDocument(context),
                    collectionName,
                    null,
                    DateTime.UtcNow.Ticks,
                    changeVector,
                    documentFlags,
                    nonPersistentFlags).Etag;

                return new DeleteOperationResult
                {
                    Collection = collectionName,
                    Etag = etag
                };
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetOrCreateLastModifiedTicks(long? lastModifiedTicks)
        {
            if (lastModifiedTicks.HasValue)
            {
                Debug.Assert(lastModifiedTicks.Value != DateTime.MinValue.Ticks, $"lastModifiedTicks cannot have DateTime.MinValue. {_name}");
                return lastModifiedTicks.Value;
            }

            return DocumentDatabase.Time.GetUtcNow().Ticks;
        }

        public long GenerateNextEtagForReplicatedTombstoneMissingDocument(DocumentsOperationContext context)
        {
            // Tombstone.DeleteEtag is not relevant, but we need a unique one here
            // we use a negative value here to indicate a missing replicated tombstone
            var newEtag = GenerateNextEtag();
            EnsureLastEtagIsPersisted(context, newEtag);
            return -newEtag;
        }

        // Note: Make sure to call this with a separator, so you won't delete "users/11" for "users/1"
        public List<DeleteOperationResult> DeleteDocumentsStartingWith(DocumentsOperationContext context, string prefix, long maxDocsToDelete = long.MaxValue, Action<Document> beforeDeleted = null)
        {
            var deleteResults = new List<DeleteOperationResult>();

            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            using (DocumentIdWorker.GetSliceFromId(context, prefix, out Slice prefixSlice))
            {
                while (true)
                {
                    if (table.SeekOnePrimaryKeyPrefix(prefixSlice, out var reader) == false)
                        break;

                    if (beforeDeleted != null)
                    {
                        var doc = TableValueToDocument(context, ref reader);

                        beforeDeleted(doc);
                    }

                    var id = TableValueToId(context, (int)DocumentsTable.Id, ref reader);

                    var deleteOperationResult = Delete(context, id, null);
                    if (deleteOperationResult != null)
                        deleteResults.Add(deleteOperationResult.Value);

                    if (--maxDocsToDelete <= 0)
                        break;
                }
            }

            return deleteResults;
        }

        public struct DeleteOperationResult
        {
            public long Etag;
            public string ChangeVector;
            public CollectionName Collection;
        }

        public long GenerateNextEtag()
        {
            return Interlocked.Increment(ref _lastEtag); // use interlocked so the GetDatabaseChangeVector can read the latest version
        }

        internal void InitializeLastEtag(Transaction tx)
        {
            _lastEtag = ReadLastEtag(tx);
        }

        public void EnsureLastEtagIsPersisted(DocumentsOperationContext context, long docEtag)
        {
            // this is called only from write tx, don't need to worry about threading to read _lastEtag
            if (docEtag != _lastEtag)
                return;
            var etagTree = context.Transaction.InnerTransaction.ReadTree(EtagsSlice);
            var etag = _lastEtag;
            using (Slice.External(context.Allocator, (byte*)&etag, sizeof(long), out Slice etagSlice))
                etagTree.Add(LastEtagSlice, etagSlice);
        }

        public (long Etag, string ChangeVector) CreateTombstone(DocumentsOperationContext context,
            Slice lowerId,
            long documentEtag,
            CollectionName collectionName,
            string docChangeVector,
            long lastModifiedTicks,
            string changeVector,
            DocumentFlags flags,
            NonPersistentDocumentFlags nonPersistentFlags)
        {
            var newEtag = GenerateNextEtag();

            if (string.IsNullOrEmpty(changeVector) &&
                nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromReplication) == false)
            {
                changeVector = ConflictsStorage.GetMergedConflictChangeVectorsAndDeleteConflicts(
                    context,
                    lowerId,
                    newEtag,
                    docChangeVector);

                if (string.IsNullOrEmpty(changeVector))
                    ChangeVectorUtils.ThrowConflictingEtag(lowerId.ToString(), docChangeVector, newEtag, Environment.Base64Id, DocumentDatabase.ServerStore.NodeTag);

                if (context.LastDatabaseChangeVector == null)
                    context.LastDatabaseChangeVector = GetDatabaseChangeVector(context);

                changeVector = ChangeVectorUtils.MergeVectors(context.LastDatabaseChangeVector, changeVector);

                context.LastDatabaseChangeVector = changeVector;
            }
            else
            {
                ConflictsStorage.DeleteConflictsFor(context, lowerId, null);
            }

            var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema,
                collectionName.GetTableName(CollectionTableType.Tombstones));

            FlagsProperlySet(flags, changeVector);

            try
            {
                using (ModifyLowerIdIfNeeded(context, table, lowerId, out var nonConflictedLowerId))
                using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
                using (Slice.From(context.Allocator, changeVector, out var cv))
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(nonConflictedLowerId);
                    tvb.Add(Bits.SwapBytes(newEtag));
                    tvb.Add(Bits.SwapBytes(documentEtag));
                    tvb.Add(context.GetTransactionMarker());
                    tvb.Add((byte)Tombstone.TombstoneType.Document);
                    tvb.Add(collectionSlice);
                    tvb.Add((int)flags);
                    tvb.Add(cv.Content.Ptr, cv.Size);
                    tvb.Add(lastModifiedTicks);
                    table.Insert(tvb);
                }
            }
            catch (VoronConcurrencyErrorException e)
            {
                var tombstoneTable = new Table(TombstonesSchema, context.Transaction.InnerTransaction);
                if (tombstoneTable.ReadByKey(lowerId, out var tvr))
                {
                    var tombstoneCollection = TableValueToId(context, (int)TombstoneTable.Collection, ref tvr);
                    var tombstoneCollectionName = ExtractCollectionName(context, tombstoneCollection);

                    if (tombstoneCollectionName != collectionName)
                    {
                        Debug.Assert(false, "Should never happened after RavenDB-14325");
                        ThrowNotSupportedExceptionForCreatingTombstoneWhenItExistsForDifferentCollection(lowerId, collectionName, tombstoneCollectionName, e);
                    }
                }

                throw;
            }

            return (newEtag, changeVector);
        }

        [Conditional("DEBUG")]
        public static void FlagsProperlySet(DocumentFlags flags, string changeVector)
        {
            var cvArray = changeVector.ToChangeVector();

            if (flags.Contain(DocumentFlags.FromClusterTransaction))
            {
                if (cvArray.Length != 1)
                {
                    Debug.Assert(false, $"FromClusterTransaction, expect change vector of length 1, {changeVector}");
                }
                if (cvArray[0].NodeTag != ChangeVectorParser.RaftInt)
                {
                    Debug.Assert(false, $"FromClusterTransaction, expect only RAFT, {changeVector}");
                }
            }

            if (cvArray.Length == 1 && cvArray[0].NodeTag == ChangeVectorParser.RaftInt)
            {
                if (flags.Contain(DocumentFlags.FromClusterTransaction) == false)
                {
                    Debug.Assert(false, $"flags must set FromClusterTransaction for the change vector: {changeVector}");
                }
            }
        }

        private IDisposable ModifyLowerIdIfNeeded(DocumentsOperationContext context, Table table, Slice lowerId, out Slice nonConflictedLowerId)
        {
            if (table.ReadByKey(lowerId, out _) == false)
            {
                nonConflictedLowerId = lowerId;
                return null;
            }

            var length = lowerId.Content.Length;
            var disposable = Slice.From(context.Allocator, lowerId.Content.Ptr, length + ConflictedTombstoneOverhead, out nonConflictedLowerId);

            *(nonConflictedLowerId.Content.Ptr + length) = SpecialChars.RecordSeparator;
            *(long*)(nonConflictedLowerId.Content.Ptr + length + sizeof(byte)) = Bits.SwapBytes(GenerateNextEtag()); // now the id will be unique
            return disposable;
        }

        // long - Etag, byte - separator char
        private const int ConflictedTombstoneOverhead = sizeof(long) + sizeof(byte);

        private static LazyStringValue UnwrapLowerIdIfNeeded(JsonOperationContext context, LazyStringValue lowerId)
        {
            if (lowerId.Size < ConflictedTombstoneOverhead + 1)
                return lowerId;

            if (lowerId[lowerId.Size - ConflictedTombstoneOverhead] == SpecialChars.RecordSeparator)
            {
                var size = lowerId.Size - ConflictedTombstoneOverhead;
                var allocated = context.GetMemory(size + 1); // we need this extra byte to mark that there is no escaping
                allocated.Address[size] = 0;

                Memory.Copy(allocated.Address, lowerId.Buffer, size);
                var lsv = context.AllocateStringValue(null, allocated.Address, size);
                lsv.AllocatedMemoryData = allocated;
                return lsv;
            }

            return lowerId;
        }

        public static bool IsTombstoneOfId(Slice tombstoneKey, Slice lowerId)
        {
            if (tombstoneKey.Size < ConflictedTombstoneOverhead + 1)
                return SliceComparer.EqualsInline(tombstoneKey, lowerId);

            if (tombstoneKey[tombstoneKey.Size - ConflictedTombstoneOverhead] == SpecialChars.RecordSeparator)
            {
                return Memory.CompareInline(tombstoneKey.Content.Ptr, lowerId.Content.Ptr, lowerId.Size) == 0;
            }

            return SliceComparer.EqualsInline(tombstoneKey, lowerId);
        }

        private void ThrowNotSupportedExceptionForCreatingTombstoneWhenItExistsForDifferentCollection(Slice lowerId, CollectionName collectionName,
            CollectionName tombstoneCollectionName, VoronConcurrencyErrorException e)
        {
            var tombstoneCleanerState = DocumentDatabase.TombstoneCleaner.GetState();
            if (tombstoneCleanerState.TryGetValue(tombstoneCollectionName.Name, out var item) && item.Documents.Component != null)
                throw new NotSupportedException($"Could not delete document '{lowerId}' from collection '{collectionName.Name}' because tombstone for that document already exists but in a different collection ('{tombstoneCollectionName.Name}'). Did you change the document's collection recently? If yes, please give some time for other system components (e.g. Indexing, Replication, Backup) and tombstone cleaner to process that change. At this point of time the component that holds the tombstone is '{item.Documents.Component}' with etag '{item.Documents.Value}' and tombstone cleaner is executed every '{DocumentDatabase.Configuration.Tombstones.CleanupInterval.AsTimeSpan.TotalMinutes}' minutes.", e);

            throw new NotSupportedException($"Could not delete document '{lowerId}' from collection '{collectionName.Name}' because tombstone for that document already exists but in a different collection ('{tombstoneCollectionName.Name}'). Did you change the document's collection recently? If yes, please give some time for other system components (e.g. Indexing, Replication, Backup) and tombstone cleaner to process that change. Tombstone cleaner is executed every '{DocumentDatabase.Configuration.Tombstones.CleanupInterval.AsTimeSpan.TotalMinutes}' minutes.", e);
        }

        public struct PutOperationResults
        {
            public string Id;
            public long Etag;
            public CollectionName Collection;
            public DateTime LastModified;
            public string ChangeVector;
            public DocumentFlags Flags;
        }

        public void DeleteWithoutCreatingTombstone(DocumentsOperationContext context, string collection, long storageId, bool isTombstone)
        {
            // we delete the data directly, without generating a tombstone, because we have a
            // conflict instead
            var tx = context.Transaction.InnerTransaction;

            var collectionObject = new CollectionName(collection);
            var collectionName = isTombstone ?
                collectionObject.GetTableName(CollectionTableType.Tombstones) :
                collectionObject.GetTableName(CollectionTableType.Documents);

            //make sure that the relevant collection tree exists
            Table table = isTombstone ?
                tx.OpenTable(TombstonesSchema, collectionName) :
                tx.OpenTable(DocsSchema, collectionName);

            table.Delete(storageId);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PutOperationResults Put(DocumentsOperationContext context, string id,
            string expectedChangeVector, BlittableJsonReaderObject document, long? lastModifiedTicks = null, string changeVector = null,
            DocumentFlags flags = DocumentFlags.None, NonPersistentDocumentFlags nonPersistentFlags = NonPersistentDocumentFlags.None)
        {
            return DocumentPut.PutDocument(context, id, expectedChangeVector, document, lastModifiedTicks, changeVector, flags, nonPersistentFlags);
        }

        public long GetNumberOfDocumentsToProcess(DocumentsOperationContext context, string collection, long afterEtag, out long totalCount)
        {
            return GetNumberOfItemsToProcess(context, collection, afterEtag, tombstones: false, totalCount: out totalCount);
        }

        public long GetNumberOfTombstonesToProcess(DocumentsOperationContext context, string collection, long afterEtag, out long totalCount)
        {
            return GetNumberOfItemsToProcess(context, collection, afterEtag, tombstones: true, totalCount: out totalCount);
        }

        private long GetNumberOfItemsToProcess(DocumentsOperationContext context, string collection, long afterEtag, bool tombstones, out long totalCount)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
            {
                totalCount = 0;
                return 0;
            }

            Table table;
            TableSchema.FixedSizeSchemaIndexDef indexDef;
            if (tombstones)
            {
                table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema,
                    collectionName.GetTableName(CollectionTableType.Tombstones));

                indexDef = TombstonesSchema.FixedSizeIndexes[CollectionEtagsSlice];
            }
            else
            {
                table = context.Transaction.InnerTransaction.OpenTable(DocsSchema,
                    collectionName.GetTableName(CollectionTableType.Documents));
                indexDef = DocsSchema.FixedSizeIndexes[CollectionEtagsSlice];
            }
            if (table == null)
            {
                totalCount = 0;
                return 0;
            }

            return table.GetNumberOfEntriesAfter(indexDef, afterEtag, out totalCount);
        }

        public long GetNumberOfDocuments()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
                return GetNumberOfDocuments(context);
        }

        public long GetNumberOfDocuments(DocumentsOperationContext context)
        {
            var fstIndex = DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice];
            var fst = context.Transaction.InnerTransaction.FixedTreeFor(fstIndex.Name, sizeof(long));
            return fst.NumberOfEntries;
        }

        public class CollectionStats
        {
            public string Name;
            public long Count;
        }

        public IEnumerable<CollectionStats> GetCollections(DocumentsOperationContext context)
        {
            foreach (var kvp in _collectionsCache)
            {
                var collectionTable = context.Transaction.InnerTransaction.OpenTable(DocsSchema, kvp.Value.GetTableName(CollectionTableType.Documents));
                //This is the case where a read transaction reading a collection cached by a later write transaction we can safly ignore it.
                if (collectionTable == null)
                {
                    if (context.Transaction.InnerTransaction.IsWriteTransaction == false)
                        continue;
                    throw new InvalidOperationException($"Cached collection {kvp.Key} is missing its table, this is likley a bug.");
                }
                yield return new CollectionStats
                {
                    Name = kvp.Key,
                    Count = collectionTable.NumberOfEntries
                };
            }
        }

        public CollectionDetails GetCollectionDetails(DocumentsOperationContext context, string collection)
        {
            CollectionDetails collectionDetails = new CollectionDetails { Name = collection, CountOfDocuments = 0, Size = new Client.Util.Size() };
            CollectionName collectionName = GetCollection(collection, throwIfDoesNotExist: false);

            if (collectionName != null)
            {
                TableReport collectionTableReport = GetReportForTable(context, DocsSchema, collectionName.GetTableName(CollectionTableType.Documents));

                collectionDetails.CountOfDocuments = collectionTableReport.NumberOfEntries;

                collectionDetails.Size.SizeInBytes = collectionTableReport.DataSizeInBytes
                    + GetReportForTable(context, RevisionsStorage.RevisionsSchema, collectionName.GetTableName(CollectionTableType.Revisions)).DataSizeInBytes
                    + GetReportForTable(context, TombstonesSchema, collectionName.GetTableName(CollectionTableType.Tombstones)).DataSizeInBytes;
            }

            return collectionDetails;
        }

        private TableReport GetReportForTable(DocumentsOperationContext context, TableSchema schema, string name, bool blnDetailed = false)
        {
            TableReport report = new TableReport(0, 0, false);
            Table table = context.Transaction.InnerTransaction.OpenTable(schema, name);

            if (table != null)
            {
                report = table.GetReport(blnDetailed);
            }

            return report;
        }

        public CollectionStats GetCollection(string collection, DocumentsOperationContext context)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
            {
                return new CollectionStats
                {
                    Name = collection,
                    Count = 0
                };
            }

            var collectionTable = context.Transaction.InnerTransaction.OpenTable(DocsSchema,
                collectionName.GetTableName(CollectionTableType.Documents));

            if (collectionTable == null)
            {
                return new CollectionStats
                {
                    Name = collection,
                    Count = 0
                };
            }

            return new CollectionStats
            {
                Name = collectionName.Name,
                Count = collectionTable.NumberOfEntries
            };
        }

        public long DeleteTombstonesBefore(DocumentsOperationContext context, string collection, long etag, long numberOfEntriesToDelete)
        {
            string tableName;

            if (collection == AttachmentsStorage.AttachmentsTombstones ||
                collection == RevisionsStorage.RevisionsTombstones)
            {
                tableName = collection;
            }
            else
            {
                var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
                if (collectionName == null)
                    return 0;

                tableName = collectionName.GetTableName(CollectionTableType.Tombstones);
            }

            var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, tableName);
            if (table == null)
                return 0;

            var deleteCount = table.DeleteBackwardFrom(TombstonesSchema.FixedSizeIndexes[CollectionEtagsSlice], etag, numberOfEntriesToDelete);
            if (_logger.IsInfoEnabled && deleteCount > 0)
                _logger.Info($"Deleted {deleteCount:#,#;;0} tombstones earlier than {etag} in {collection}");
            if (deleteCount > 0)
                EnsureLastEtagIsPersisted(context, etag);

            return deleteCount;
        }

        public IEnumerable<string> GetTombstoneCollections(Transaction transaction)
        {
            yield return AttachmentsStorage.AttachmentsTombstones;
            yield return RevisionsStorage.RevisionsTombstones;

            using (var it = transaction.LowLevelTransaction.RootObjects.Iterate(false))
            {
                it.SetRequiredPrefix(TombstonesPrefix);

                if (it.Seek(TombstonesPrefix) == false)
                    yield break;

                do
                {
                    var tombstoneCollection = it.CurrentKey.ToString();
                    yield return tombstoneCollection.Substring(TombstonesPrefix.Size);
                }
                while (it.MoveNext());
            }
        }


        public ConflictStatus GetConflictStatus(string remote, string local)
        {
            return GetConflictStatus(remote, local, out _);
        }

        public ConflictStatus GetConflictStatus(string remote, string local, out bool skipValidation)
        {
            skipValidation = false;
            var originalStatus = ChangeVectorUtils.GetConflictStatus(remote, local);
            if (originalStatus == ConflictStatus.Conflict && HasUnusedDatabaseIds())
            {
                // We need to distinguish between few cases here
                // let's assume that node C was removed

                // our local change vector is     A:10, B:10, C:10
                // case 1: incoming change vector A:10, B:10, C:11  -> update           (original: update, after: already merged)
                // case 2: incoming change vector A:11, B:10, C:10  -> update           (original: update, after: update)
                // case 3: incoming change vector A:11, B:10        -> update           (original: conflict, after: update)
                // case 4: incoming change vector A:10, B:10        -> already merged   (original: already merged, after: already merged)

                // our local change vector is     A:11, B:10
                // case 1: incoming change vector A:10, B:10, C:10 -> conflict              (original: conflict, after: already merged)        
                // case 2: incoming change vector A:10, B:11, C:10 -> conflict              (original: conflict, after: conflict)
                // case 3: incoming change vector A:11, B:10, C:10 -> update                (original: update, after: already merged)
                // case 4: incoming change vector A:11, B:12, C:10 -> update                (original: conflict, after: update)

                var original = ChangeVectorUtils.GetConflictStatus(remote, local);
                TryRemoveUnusedIds(ref remote);
                skipValidation = TryRemoveUnusedIds(ref local);
                var after = ChangeVectorUtils.GetConflictStatus(remote, local);

                if (after == ConflictStatus.AlreadyMerged)
                    return original;
                return after;
            }

            return originalStatus;
        }

        public static IEnumerable<KeyValuePair<string, long>> GetAllReplicatedEtags(DocumentsOperationContext context)
        {
            var readTree = context.Transaction.InnerTransaction.ReadTree(LastReplicatedEtagsSlice);
            using (var it = readTree.Iterate(true))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    yield break;

                do
                {
                    var dbId = it.CurrentKey.ToString();
                    yield return new KeyValuePair<string, long>(dbId, it.CreateReaderForCurrent().ReadLittleEndianInt64());
                }
                while (it.MoveNext());
            }
        }

        public static long GetLastReplicatedEtagFrom(DocumentsOperationContext context, string dbId)
        {
            var readTree = context.Transaction.InnerTransaction.ReadTree(LastReplicatedEtagsSlice);
            var readResult = readTree.Read(dbId);
            if (readResult == null)
                return 0;

            return readResult.Reader.ReadLittleEndianInt64();
        }

        public static void SetLastReplicatedEtagFrom(DocumentsOperationContext context, string dbId, long etag)
        {
            var etagsTree = context.Transaction.InnerTransaction.CreateTree(LastReplicatedEtagsSlice);
            using (Slice.From(context.Allocator, dbId, out Slice dbIdSlice))
            using (Slice.External(context.Allocator, (byte*)&etag, sizeof(long), out Slice etagSlice))
            {
                etagsTree.Add(dbIdSlice, etagSlice);
            }
        }

        public CollectionName GetCollection(string collection, bool throwIfDoesNotExist)
        {
            if (_collectionsCache.TryGetValue(collection, out CollectionName collectionName) == false && throwIfDoesNotExist)
                throw new InvalidOperationException($"There is no collection for '{collection}'.");

            return collectionName;
        }

        public CollectionName ExtractCollectionName(DocumentsOperationContext context, BlittableJsonReaderObject document)
        {
            var originalCollectionName = CollectionName.GetCollectionName(document);
            return ExtractCollectionName(context, originalCollectionName);
        }

        public CollectionName ExtractCollectionName(DocumentsOperationContext context, string collectionName)
        {
            if (_collectionsCache.TryGetValue(collectionName, out CollectionName name))
                return name;

            if (context.Transaction == null)
            {
                ThrowNoActiveTransactionException(); //this throws, return null in the next row is there so intellisense will be happy
                return null;
            }

            if (context.Transaction.TryGetFromCache(collectionName, out name))
            {
                // for documents with case insensitive collections that were created on the same transaction
                return name;
            }

            var collections = context.Transaction.InnerTransaction.OpenTable(CollectionsSchema, CollectionsSlice);
            if (collections == null)
                throw new InvalidOperationException("Should never happen!");

            name = new CollectionName(collectionName);
            using (DocumentIdWorker.GetStringPreserveCase(context, collectionName, out Slice collectionSlice))
            {
                using (collections.Allocate(out TableValueBuilder tvr))
                {
                    tvr.Add(collectionSlice);
                    collections.Set(tvr);
                }

                context.Transaction.AddToCache(collectionName, name);

                DocsSchema.Create(context.Transaction.InnerTransaction, name.GetTableName(CollectionTableType.Documents), 16);
                TombstonesSchema.Create(context.Transaction.InnerTransaction, name.GetTableName(CollectionTableType.Tombstones), 16);

                // Add to cache ONLY if the transaction was committed.
                // this would prevent NREs next time a PUT is run,since if a transaction
                // is not committed, DocsSchema and TombstonesSchema will not be actually created..
                // has to happen after the commit, but while we are holding the write tx lock
                context.Transaction.InnerTransaction.LowLevelTransaction.BeforeCommitFinalization += _ =>
                {
                    var collectionNames = new Dictionary<string, CollectionName>(_collectionsCache, OrdinalIgnoreCaseStringStructComparer.Instance)
                    {
                        [name.Name] = name
                    };
                    _collectionsCache = collectionNames;
                };
            }
            return name;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope GetEtagAsSlice(DocumentsOperationContext context, long etag, out Slice slice)
        {
            var scope = context.Allocator.Allocate(sizeof(long), out var keyMem);
            var swapped = Bits.SwapBytes(etag);
            Memory.Copy(keyMem.Ptr, (byte*)&swapped, sizeof(long));
            slice = new Slice(SliceOptions.Key, keyMem);
            return scope;
        }

        private static void ThrowNoActiveTransactionException()
        {
            throw new InvalidOperationException("This method requires active transaction, and no active transactions in the current context...");
        }

        private IEnumerable<string> IterateCollectionNames(Transaction tx, JsonOperationContext context)
        {
            var collections = tx.OpenTable(CollectionsSchema, CollectionsSlice);
            foreach (var tvr in collections.SeekByPrimaryKey(Slices.BeforeAllKeys, 0))
            {
                var collection = TableValueToId(context, (int)CollectionsTable.Name, ref tvr.Reader);
                yield return collection.ToString();
            }
        }

        private Dictionary<string, CollectionName> ReadCollections(Transaction tx)
        {
            var result = new Dictionary<string, CollectionName>(OrdinalIgnoreCaseStringStructComparer.Instance);

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var collections = tx.OpenTable(CollectionsSchema, CollectionsSlice);
                foreach (var tvr in collections.SeekByPrimaryKey(Slices.BeforeAllKeys, 0))
                {
                    var collection = TableValueToId(context, (int)CollectionsTable.Name, ref tvr.Reader);
                    var collectionName = new CollectionName(collection);
                    result.Add(collection, collectionName);

                    var documentsTree = tx.ReadTree(collectionName.GetTableName(CollectionTableType.Documents), RootObjectType.Table);
                    NewPageAllocator.MaybePrefetchSections(documentsTree, tx.LowLevelTransaction);

                    var tombstonesTree = tx.ReadTree(collectionName.GetTableName(CollectionTableType.Tombstones), RootObjectType.Table);
                    NewPageAllocator.MaybePrefetchSections(tombstonesTree, tx.LowLevelTransaction);
                }
            }

            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long TableValueToEtag(int index, ref TableValueReader tvr)
        {
            var ptr = tvr.Read(index, out _);
            var etag = Bits.SwapBytes(*(long*)ptr);
            return etag;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long TableValueToLong(int index, ref TableValueReader tvr)
        {
            var ptr = tvr.Read(index, out _);
            return *(long*)ptr;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static DocumentFlags TableValueToFlags(int index, ref TableValueReader tvr)
        {
            return *(DocumentFlags*)tvr.Read(index, out _);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static short TableValueToShort(int index, string name, ref TableValueReader tvr)
        {
            var value = *(short*)tvr.Read(index, out int size);
            if (size != sizeof(short))
                ThrowInvalidShortSize(name, size);
            return value;
        }

        private static void ThrowInvalidShortSize(string name, int size)
        {
            throw new InvalidOperationException($"{name} size is invalid, expected short but got {size}.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static DateTime TableValueToDateTime(int index, ref TableValueReader tvr)
        {
            return new DateTime(*(long*)tvr.Read(index, out _), DateTimeKind.Utc);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static LazyStringValue TableValueToString(JsonOperationContext context, int index, ref TableValueReader tvr)
        {
            var ptr = tvr.Read(index, out int size);
            return context.AllocateStringValue(null, ptr, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string TableValueToChangeVector(JsonOperationContext context, int index, ref TableValueReader tvr)
        {
            var ptr = tvr.Read(index, out int size);
            return Encodings.Utf8.GetString(ptr, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static LazyStringValue TableValueToId(JsonOperationContext context, int index, ref TableValueReader tvr)
        {
            var ptr = tvr.Read(index, out _);
            return context.GetLazyStringValue(ptr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope TableValueToSlice(
            DocumentsOperationContext context, int index, ref TableValueReader tvr, out Slice slice)
        {
            var ptr = tvr.Read(index, out int size);
            return Slice.From(context.Allocator, ptr, size, ByteStringType.Immutable, out slice);
        }
    }

    public enum TableType : byte
    {
        None = 0,
        Documents = 1,
        Revisions = 2,
        Conflicts = 3,
        LegacyCounter = 4,
        Counters = 5,
        TimeSeries = 6
    }
}
