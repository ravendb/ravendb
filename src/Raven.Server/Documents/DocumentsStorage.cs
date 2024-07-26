using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Config;
using Raven.Server.Documents.DataArchival;
using Raven.Server.Documents.Expiration;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.Documents.Refresh;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Revisions;
using Raven.Server.Documents.Sharding;
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
using static Raven.Server.Documents.Schemas.Collections;
using static Raven.Server.Documents.Schemas.Documents;
using static Raven.Server.Documents.Schemas.Tombstones;

namespace Raven.Server.Documents
{
    public unsafe partial class DocumentsStorage : IDisposable
    {
        public TableSchema DocsSchema;
        public TableSchema CompressedDocsSchema;
        public TableSchema TombstonesSchema;

        protected TableSchema AttachmentsSchema;
        protected TableSchema ConflictsSchema;
        public TableSchema CountersSchema;
        public TableSchema CounterTombstonesSchema;
        protected TableSchema TimeSeriesSchema;
        protected TableSchema TimeSeriesDeleteRangesSchema;
        public TableSchema RevisionsSchema;
        public TableSchema CompressedRevisionsSchema;

        public static readonly TableSchema CollectionsSchema = Schemas.Collections.Current;
        public readonly DocumentDatabase DocumentDatabase;
        public DocumentsContextPool ContextPool;
        public RevisionsStorage RevisionsStorage;
        public ExpirationStorage ExpirationStorage;
        public RefreshStorage RefreshStorage;
        public DataArchivalStorage DataArchivalStorage;
        public ConflictsStorage ConflictsStorage;
        public AttachmentsStorage AttachmentsStorage;
        public CountersStorage CountersStorage;
        public TimeSeriesStorage TimeSeriesStorage;
        public DocumentPutAction DocumentPut;
        public StorageEnvironment Environment { get; private set; }

        public Action<Transaction> OnBeforeCommit { get; protected set; }

        protected Dictionary<string, CollectionName> _collectionsCache;
        private static readonly Slice LastReplicatedEtagsSlice;
        private static readonly Slice EtagsSlice;
        private static readonly Slice LastEtagSlice;
        private static readonly Slice LastCompletedClusterTransactionIndexSlice;
        private static readonly Slice GlobalTreeSlice;
        private static readonly Slice GlobalChangeVectorSlice;
        private static readonly Slice GlobalFullChangeVectorSlice;
        private readonly Action<LogMode, string> _addToInitLog;

        private readonly Logger _logger;
        private readonly string _name;

        // this is only modified by write transactions under lock
        // no need to use thread safe ops
        private long _lastEtag;
        
        static DocumentsStorage()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "Etags", ByteStringType.Immutable, out EtagsSlice);
                Slice.From(ctx, "LastEtag", ByteStringType.Immutable, out LastEtagSlice);
                Slice.From(ctx, "LastReplicatedEtags", ByteStringType.Immutable, out LastReplicatedEtagsSlice);
                Slice.From(ctx, "LastCompletedClusterTransactionIndex", ByteStringType.Immutable, out LastCompletedClusterTransactionIndexSlice);
                Slice.From(ctx, "GlobalTree", ByteStringType.Immutable, out GlobalTreeSlice);
                Slice.From(ctx, "GlobalChangeVector", ByteStringType.Immutable, out GlobalChangeVectorSlice);
                Slice.From(ctx, "GlobalFullChangeVector", ByteStringType.Immutable, out GlobalFullChangeVectorSlice);
            }
        }

        public DocumentsStorage(DocumentDatabase documentDatabase, Action<LogMode, string> addToInitLog)
        {
            DocumentDatabase = documentDatabase;
            SetDocumentsStorageSchemas();
            _name = DocumentDatabase.Name;
            _logger = LoggingSource.Instance.GetLogger<DocumentsStorage>(documentDatabase.Name);
            _addToInitLog = addToInitLog;
        }

        protected virtual void SetDocumentsStorageSchemas()
        {
            DocsSchema = Schemas.Documents.DocsSchemaBase;
            TombstonesSchema = Schemas.Tombstones.TombstonesSchemaBase;
            CompressedDocsSchema = Schemas.Documents.CompressedDocsSchemaBase;

            AttachmentsSchema = Schemas.Attachments.AttachmentsSchemaBase;
            ConflictsSchema = Schemas.Conflicts.ConflictsSchemaBase;
            CountersSchema = Schemas.Counters.CountersSchemaBase;
            CounterTombstonesSchema = Schemas.CounterTombstones.CounterTombstonesSchemaBase;

            TimeSeriesSchema = Schemas.TimeSeries.TimeSeriesSchemaBase;
            TimeSeriesDeleteRangesSchema = Schemas.DeletedRanges.DeleteRangesSchemaBase;

            RevisionsSchema = Schemas.Revisions.RevisionsSchemaBase;
            CompressedRevisionsSchema = Schemas.Revisions.CompressedRevisionsSchemaBase;
        }

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

            if (DocumentDatabase.Configuration.Core.RunInMemory == false)
            {
                string disableMarkerPath = DocumentDatabase.Configuration.Core.DataDirectory.Combine("disable.marker").FullPath;
                if (File.Exists(disableMarkerPath))
                {
                    throw new DatabaseDisabledException(
                        $"Unable to open database: '{_name}', it has been manually disabled via the file: '{disableMarkerPath}'. To re-enable, remove the disable.marker and reload the database.");
                }

            }

            var options = GetStorageEnvironmentOptionsFromConfiguration(DocumentDatabase.Configuration, DocumentDatabase.IoChanges, DocumentDatabase.CatastrophicFailureNotification);

            options.OnNonDurableFileSystemError += DocumentDatabase.HandleNonDurableFileSystemError;
            options.OnRecoveryError += DocumentDatabase.HandleOnDatabaseRecoveryError;
            options.OnIntegrityErrorOfAlreadySyncedData += DocumentDatabase.HandleOnDatabaseIntegrityErrorOfAlreadySyncedData;
            options.OnRecoverableFailure += DocumentDatabase.HandleRecoverableFailure;

            options.GenerateNewDatabaseId = generateNewDatabaseId;
            options.CompressTxAboveSizeInBytes = DocumentDatabase.Configuration.Storage.CompressTxAboveSize.GetValue(SizeUnit.Bytes);
            options.ForceUsing32BitsPager = DocumentDatabase.Configuration.Storage.ForceUsing32BitsPager;
            options.EnablePrefetching = DocumentDatabase.Configuration.Storage.EnablePrefetching;
            options.DiscardVirtualMemory = DocumentDatabase.Configuration.Storage.DiscardVirtualMemory;
            options.TimeToSyncAfterFlushInSec = (int)DocumentDatabase.Configuration.Storage.TimeToSyncAfterFlush.AsTimeSpan.TotalSeconds;
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
            options.MaxNumberOfRecyclableJournals = DocumentDatabase.Configuration.Storage.MaxNumberOfRecyclableJournals;

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

        private void Initialize(StorageEnvironmentOptions options)
        {
            options.SchemaVersion = SchemaUpgrader.CurrentVersion.DocumentsVersion;
            options.SchemaUpgrader = SchemaUpgrader.Upgrader(SchemaUpgrader.StorageType.Documents, null, this, null);
            try
            {
                DirectoryExecUtils.SubscribeToOnDirectoryInitializeExec(options, DocumentDatabase.Configuration.Storage, DocumentDatabase.Name, DirectoryExecUtils.EnvironmentType.Database, _logger);

                ContextPool = new DocumentsContextPool(DocumentDatabase);
                Environment = StorageLoader.OpenEnvironment(options, StorageEnvironmentWithType.StorageEnvironmentType.Documents);

                Environment.NewTransactionCreated += tx =>
                {
                    if (tx.Flags == TransactionFlags.ReadWrite)
                    {
                        tx.LastChanceToReadFromWriteTransactionBeforeCommit += ComputeTransactionCache_BeforeCommit;
                    }
                };

                using (var tx = Environment.WriteTransaction())
                {
                    NewPageAllocator.MaybePrefetchSections(
                        tx.LowLevelTransaction.RootObjects,
                        tx.LowLevelTransaction);

                    tx.CreateTree(TableSchema.CompressionDictionariesSlice);
                    tx.CreateTree(DocsSlice);
                    tx.CreateTree(LastReplicatedEtagsSlice);
                    tx.CreateTree(GlobalTreeSlice);
                    tx.CreateTree(ShardedDocumentsStorage.BucketStatsSlice);

                    CollectionsSchema.Create(tx, CollectionsSlice, 32);

                    RevisionsStorage = new RevisionsStorage(DocumentDatabase, tx, RevisionsSchema, CompressedRevisionsSchema);
                    ExpirationStorage = new ExpirationStorage(DocumentDatabase, tx);
                    RefreshStorage = new RefreshStorage(DocumentDatabase, tx);
                    DataArchivalStorage = new DataArchivalStorage(DocumentDatabase, tx);
                    ConflictsStorage = new ConflictsStorage(DocumentDatabase, tx, ConflictsSchema);
                    AttachmentsStorage = new AttachmentsStorage(DocumentDatabase, tx, AttachmentsSchema);
                    CountersStorage = new CountersStorage(DocumentDatabase, tx, CountersSchema, CounterTombstonesSchema);
                    TimeSeriesStorage = new TimeSeriesStorage(DocumentDatabase, tx, TimeSeriesSchema, TimeSeriesDeleteRangesSchema);

                    DocumentPut = CreateDocumentPutAction();

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

        protected virtual DocumentPutAction CreateDocumentPutAction()
        {
            return new DocumentPutAction(this, DocumentDatabase);
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
                ConflictsCount =  ConflictsStorage.GetNumberOfConflicts(tx)
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
            // this is used to remember the metadata about collections / documents for
            // common operations. Thread safety is inherited from the voron transaction
            tx.LowLevelTransaction.UpdateClientState(currentCache);
        }


        public static ChangeVector GetDatabaseChangeVector(DocumentsOperationContext context)
        {
            return context.GetChangeVector(GetDatabaseChangeVector(context.Transaction.InnerTransaction));
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

        public (ChangeVector ChangeVector, long Etag) GetNewChangeVector(DocumentsOperationContext context)
        {
            var etag = GenerateNextEtag();
            return (GetNewChangeVector(context, etag), etag);
        }

        public ChangeVector GetNewChangeVector(DocumentsOperationContext context, long newEtag)
        {
            var changeVector = context.LastDatabaseChangeVector ??
                               (context.LastDatabaseChangeVector = GetDatabaseChangeVector(context));

            context.SkipChangeVectorValidation = changeVector.TryRemoveIds(UnusedDatabaseIds, context, out changeVector);

            if (changeVector.IsNullOrEmpty)
            {
                context.LastDatabaseChangeVector = ChangeVectorUtils.NewChangeVector(DocumentDatabase, newEtag, context);
                return context.LastDatabaseChangeVector;
            }

            var result = ChangeVectorUtils.TryUpdateChangeVector(DocumentDatabase, changeVector, newEtag);
            if (result.IsValid)
            {
                context.LastDatabaseChangeVector = context.GetChangeVector(result.ChangeVector);
            }

            return context.LastDatabaseChangeVector;
        }

        public void SetDatabaseChangeVector(DocumentsOperationContext context, ChangeVector changeVector)
        {
            SetFullDatabaseChangeVector(context, changeVector);

            if (changeVector.TryRemoveIds(UnusedDatabaseIds, context, out changeVector) == false)
                ThrowOnNotUpdatedChangeVector(context, changeVector);

            var tree = context.Transaction.InnerTransaction.ReadTree(GlobalTreeSlice);
            using (Slice.From(context.Allocator, changeVector, out var slice))
            {
                tree.Add(GlobalChangeVectorSlice, slice);
            }
        }

        public static string GetFullDatabaseChangeVector(DocumentsOperationContext context)
        {
            var tx = context.Transaction.InnerTransaction;
            var tree = tx.ReadTree(GlobalTreeSlice);
            var val = tree.Read(GlobalFullChangeVectorSlice);
            if (val == null)
            {
                return GetDatabaseChangeVector(context);
            }
            return Encodings.Utf8.GetString(val.Reader.Base, val.Reader.Length);

        }

        public void SetFullDatabaseChangeVector(DocumentsOperationContext context, string changeVector)
        {
            var fullChangeVector = ChangeVectorUtils.MergeVectors(changeVector, GetFullDatabaseChangeVector(context));

            var tree = context.Transaction.InnerTransaction.ReadTree(GlobalTreeSlice);
            using (Slice.From(context.Allocator, fullChangeVector, out var slice))
            {
                tree.Add(GlobalFullChangeVectorSlice, slice);
            }
        }

        [Conditional("DEBUG")]
        private static void ThrowOnNotUpdatedChangeVector(DocumentsOperationContext context, ChangeVector changeVector)
        {
            var globalChangeVector = GetDatabaseChangeVector(context);

            if (context.SkipChangeVectorValidation)
                return;

            if (globalChangeVector.IsEqual(changeVector) == false &&
                globalChangeVector.IsNullOrEmpty == false &&
                // globalChangeVector.ToChangeVector().OrderByDescending(x => x).SequenceEqual(changeVector.ToChangeVector().OrderByDescending(x => x)) == false &&
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
                if (tx.LowLevelTransaction.TryGetClientState(out DocumentTransactionCache cache))
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
                if (tx.LowLevelTransaction.TryGetClientState(out DocumentTransactionCache cache))
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
                if (tx.LowLevelTransaction.TryGetClientState(out DocumentTransactionCache cache))
                {
                    return cache.LastConflictEtag;
                }
            }
            return ReadLastEtagFrom(tx, Schemas.Conflicts.AllConflictedDocsEtagsSlice);
        }

        public static long ReadLastRevisionsEtag(Transaction tx)
        {
            if (tx.IsWriteTransaction == false)
            {
                if (tx.LowLevelTransaction.TryGetClientState(out DocumentTransactionCache cache))
                {
                    return cache.LastRevisionsEtag;
                }
            }
            return ReadLastEtagFrom(tx, Schemas.Revisions.AllRevisionsEtagsSlice);
        }

        public long ReadLastAttachmentsEtag(Transaction tx)
        {
            if (tx.IsWriteTransaction == false)
            {
                if (tx.LowLevelTransaction.TryGetClientState(out DocumentTransactionCache cache))
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
                if (tx.LowLevelTransaction.TryGetClientState(out DocumentTransactionCache cache))
                {
                    return cache.LastCounterEtag;
                }
            }
            return ReadLastEtagFrom(tx, Schemas.Counters.AllCountersEtagSlice);
        }

        public static long ReadLastTimeSeriesEtag(Transaction tx)
        {
            if (tx.IsWriteTransaction == false)
            {
                if (tx.LowLevelTransaction.TryGetClientState(out DocumentTransactionCache cache))
                {
                    return cache.LastTimeSeriesEtag;
                }
            }
            return ReadLastEtagFrom(tx, Schemas.TimeSeries.AllTimeSeriesEtagSlice);
        }

        private static long ReadLastEtagFrom(Transaction tx, Slice name)
        {
            var fst = new FixedSizeTree(tx.LowLevelTransaction, tx.LowLevelTransaction.RootObjects, 
                                        name, sizeof(long), clone: false);

            using var it = fst.Iterate();
            return it.SeekToLast() ? it.CurrentKey : 0;
        }

        public long ReadLastEtag(Transaction tx)
        {
            if (tx.IsWriteTransaction == false)
            {
                if (tx.LowLevelTransaction.TryGetClientState(out DocumentTransactionCache cache))
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

        public static long ReadLastCompletedClusterTransactionIndex(Transaction tx)
        {
            if (tx == null)
                throw new InvalidOperationException("No active transaction found in the context, and at least read transaction is needed");
            var tree = tx.ReadTree(GlobalTreeSlice);
            if (tree == null)
            {
                return 0;
            }
            var readResult = tree.Read(LastCompletedClusterTransactionIndexSlice);
            if (readResult == null)
            {
                return 0;
            }

            return readResult.Reader.ReadLittleEndianInt64();
        }

        public void SetLastCompletedClusterTransactionIndex(DocumentsOperationContext context, long index)
        {
            var tree = context.Transaction.InnerTransaction.CreateTree(GlobalTreeSlice);
            using (Slice.External(context.Allocator, (byte*)&index, sizeof(long), out Slice indexSlice))
                tree.Add(LastCompletedClusterTransactionIndexSlice, indexSlice);
        }

        public IEnumerable<Document> GetDocumentsStartingWith(DocumentsOperationContext context, string idPrefix, string startAfterId,
            long start, long take, string collection, Reference<long> skippedResults, DocumentFields fields = DocumentFields.All, CancellationToken token = default)
        {
            var isAllDocs = collection == Constants.Documents.Collections.AllDocumentsCollection;
            var isEmptyCollection = collection == Constants.Documents.Collections.EmptyCollection;
            var requestedDataField = fields.HasFlag(DocumentFields.Data);
            if (isAllDocs == false && requestedDataField == false)
                fields |= DocumentFields.Data;

            using (var collectionAsLazyString = context.GetLazyString(collection))
            {
                // we request ALL documents that start with `idPrefix` and filter it here by the collection name
                foreach (var doc in GetDocumentsStartingWith(context, idPrefix, null, null, startAfterId, start, take: long.MaxValue, fields: fields))
                {
                    token.ThrowIfCancellationRequested();

                    if (isAllDocs)
                    {
                        if (take-- < 0)
                            break;

                        yield return doc;
                        continue;
                    }

                    if (IsCollectionMatch(doc) == false)
                    {
                        skippedResults.Value++;
                        doc.Dispose();
                        continue;
                    }

                    if (requestedDataField == false)
                    {
                        doc.Data.Dispose();
                        doc.Data = null;
                    }

                    if (take-- <= 0)
                    {
                        doc.Dispose();
                        break;
                    }

                    yield return doc;
                }

                bool IsCollectionMatch(Document doc)
                {
                    if (doc.TryGetMetadata(out var metadata) == false)
                        return false;

                    if (metadata.TryGet(Constants.Documents.Metadata.Collection, out LazyStringValue c) == false)
                        return false;

                    if (c != null)
                    {
                        if (collectionAsLazyString.EqualsOrdinalIgnoreCase(c) == false)
                            return false;
                    }
                    else
                    {
                        if (isEmptyCollection == false)
                            return false;
                    }

                    return true;
                }
            }
        }

        public IEnumerable<Document> GetDocumentsStartingWith(DocumentsOperationContext context, string idPrefix, string matches, string exclude, string startAfterId,
            long start, long take, Reference<long> skip = null, DocumentFields fields = DocumentFields.All, CancellationToken token = default)
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
                    token.ThrowIfCancellationRequested();

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

            var table = context.Transaction.InnerTransaction.OpenTable(DocumentDatabase.GetDocsSchemaForCollection(collectionName),
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

        internal TestingStuff _forTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (_forTestingPurposes != null)
                return _forTestingPurposes;

            return _forTestingPurposes = new TestingStuff(DocsSchema);
        }

        internal sealed class TestingStuff
        {
            private readonly TableSchema _docsSchema;

            internal TestingStuff(TableSchema docsSchema)
            {
                _docsSchema = docsSchema;
            }

            public ManualResetEventSlim DelayDocumentLoad;
            public Action<string> OnBeforeOpenTableWhenPutDocumentWithSpecificId { get; set; }

            public bool DisableDebugAssertionForTableThrowNotOwned;

            public bool? IsDocumentCompressed(DocumentsOperationContext context, Slice lowerDocumentId, out bool? isLargeValue)
            {
                var table = new Table(_docsSchema, context.Transaction.InnerTransaction);
                return table.ForTestingPurposesOnly().IsTableValueCompressed(lowerDocumentId, out isLargeValue);
            }
        }
        
        public IEnumerable<Document> GetDocumentsFrom(DocumentsOperationContext context, long etag, long start, long take, DocumentFields fields = DocumentFields.All, EventHandler<InvalidOperationException> onCorruptedDataHandler = null)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction, onCorruptedDataHandler);

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

        public IEnumerable<DocumentReplicationItem> GetDocumentsFrom(DocumentsOperationContext context, long etag, DocumentFields fields = DocumentFields.All)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice], etag, 0))
            {
                yield return DocumentReplicationItem.From(TableValueToDocument(context, ref result.Reader, fields), context);
            }
        }

        public IEnumerable<Document>  GetDocuments(DocumentsOperationContext context, IEnumerable<Slice> ids, long start, long take)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            foreach (var id in ids)
            {
                // id must be lowercased
                if (table.ReadByKey(id, out TableValueReader reader) == false)
                    continue;
                
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

        public IEnumerable<Document> GetDocuments(DocumentsOperationContext context, IEnumerable<string> ids, long start, long take)
        {
            var listOfIds = new List<Slice>();
            foreach (var id in ids)
            {
                Slice.From(context.Allocator, id.ToLowerInvariant(), out Slice slice);
                listOfIds.Add(slice);
            }

            return GetDocuments(context, listOfIds, start, take);
        }

        public IEnumerable<Document> GetDocumentsForCollection(DocumentsOperationContext context, IEnumerable<Slice> ids, string collection, long start, long take)
        {
            // we'll fetch all documents and do the filtering here since we must check the collection name
            foreach (var doc in GetDocuments(context, ids, start, int.MaxValue))
            {
                if (collection == Constants.Documents.Collections.AllDocumentsCollection)
                {
                    yield return doc;
                    continue;
                }

                if (doc.TryGetMetadata(out var metadata) == false)
                {
                    continue;
                }
                if (metadata.TryGet(Constants.Documents.Metadata.Collection, out string c) == false)
                {
                    continue;
                }
                if (string.Equals(c, collection, StringComparison.OrdinalIgnoreCase) == false)
                {
                    continue;
                }

                if (take-- <= 0)
                    continue; // we need to calculate totalCount correctly

                yield return doc;
            }
        }

        public IEnumerable<Document> GetDocumentsFrom(DocumentsOperationContext context, string collection, long etag, long start, long take, DocumentFields fields = DocumentFields.All)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                yield break;

            var table = context.Transaction.InnerTransaction.OpenTable(DocumentDatabase.GetDocsSchemaForCollection(collectionName),
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

        public DocumentOrTombstone GetDocumentOrTombstone(DocumentsOperationContext context, string id, DocumentFields fields = DocumentFields.All, bool throwOnConflict = true)
        {
            if (string.IsNullOrWhiteSpace(id))
                throw new ArgumentException("Argument is null or whitespace", nameof(id));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Put", nameof(context));

            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
            {
                return GetDocumentOrTombstone(context, lowerId, fields, throwOnConflict);
            }
        }

        public struct DocumentOrTombstone
        {
            public Document Document;
            public Tombstone Tombstone;
            public bool Missing => Document == null && Tombstone == null;
        }

        public DocumentOrTombstone GetDocumentOrTombstone(DocumentsOperationContext context, Slice lowerId, DocumentFields fields = DocumentFields.All, bool throwOnConflict = true)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                return default(DocumentOrTombstone); // never hit
            }

            try
            {
                var doc = Get(context, lowerId, fields);
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

            Tombstone mostRecent = null;
            foreach (var (tombstoneKey, tvh) in tombstoneTable.SeekByPrimaryKeyPrefix(lowerId, Slices.Empty, 0))
            {
                if (IsTombstoneOfId(tombstoneKey, lowerId))
                {
                    var current = TableValueToTombstone(context, ref tvh.Reader);
                    if (mostRecent == null || 
                        GetConflictStatus(context, current.ChangeVector, mostRecent.ChangeVector, ChangeVectorMode.Version) == ConflictStatus.Update)
                    {
                        using (var _ = mostRecent)
                        {
                            mostRecent = current;
                        }
                    }
                }
            }

            return new DocumentOrTombstone
            {
                Tombstone = mostRecent
            };
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Document Get(DocumentsOperationContext context, ReadOnlyMemory<char> id, DocumentFields fields = DocumentFields.All, bool throwOnConflict = true)
        {
            if (id.IsEmpty)
                throw new ArgumentException("Argument is null", nameof(id));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Get", nameof(context));

            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
            {
                return Get(context, lowerId, fields, throwOnConflict);
            }
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

        public IEnumerable<string> GetAllIds(DocumentsOperationContext context)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[AllDocsEtagsSlice], 0, 0))
            {
                yield return TableValueToId(context, (int)DocumentsTable.Id, ref result.Reader);
            }
        }

        public (int ActualSize, int AllocatedSize, bool IsCompressed)? GetDocumentMetrics(DocumentsOperationContext context, string id)
        {
            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
            {
                var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

                if (table.ReadByKey(lowerId, out var tvr) == false)
                {
                    return null;
                }

                var info = table.GetInfoFor(tvr.Id);

                return (tvr.Size, info.AllocatedSize, info.IsCompressed);
            }
        }

        public bool GetTableValueReaderForDocument(DocumentsOperationContext context, Slice lowerId, bool throwOnConflict, out TableValueReader tvr)
        {
            var table = new Table(DocsSchema, context.Transaction.InnerTransaction);

            if (table.ReadByKey(lowerId, out tvr) == false)
            {
                if (throwOnConflict && ConflictsStorage.NumberOfConflicts(context) > 0)
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

        public GetTombstonesPreviewResult GetTombstonesPreviewResult(DocumentsOperationContext context, long etag, long start, long take)
        {
            var table = new Table(TombstonesSchema, context.Transaction.InnerTransaction);

            var tombstones = new List<Tombstone>();
            foreach (var result in table.SeekForwardFrom(TombstonesSchema.FixedSizeIndexes[AllTombstonesEtagsSlice], etag, start))
            {
                if (take-- <= 0)
                    break;

                var tombstone = TableValueToTombstone(context, ref result.Reader);
                tombstones.Add(tombstone.CloneInternal(context));
            }

            return new GetTombstonesPreviewResult
            {
                Tombstones = tombstones
            };
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

        public IEnumerable<ReplicationBatchItem> GetTombstonesFrom(DocumentsOperationContext context, long etag, bool revisionTombstonesWithId = true)
        {
            var table = new Table(TombstonesSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(TombstonesSchema.FixedSizeIndexes[AllTombstonesEtagsSlice], etag, 0))
            {
                var tombstoneItem = TombstoneReplicationItem.From(context, TableValueToTombstone(context, ref result.Reader));

                if (revisionTombstonesWithId == false && tombstoneItem is RevisionTombstoneReplicationItem revisionTombstone)
                    revisionTombstone.StripDocumentIdFromKeyIfNeeded(context);

                yield return tombstoneItem;
            }
        }

        public IEnumerable<Tombstone> GetAttachmentTombstonesFrom(
            DocumentsOperationContext context,
            long etag,
            long start,
            long take)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, Schemas.Attachments.AttachmentsTombstones);

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

        private Table GetTombstoneTableForCollection(DocumentsOperationContext context, string collection)
        {
            string tableName;

            if (collection == Schemas.Attachments.AttachmentsTombstones ||
                collection == Schemas.Revisions.RevisionsTombstones)
            {
                tableName = collection;
            }
            else
            {
                var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
                if (collectionName == null)
                    return null;

                tableName = collectionName.GetTableName(CollectionTableType.Tombstones);
            }

            var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, tableName);
            return table;
        }

        public long TombstonesCountForCollection(DocumentsOperationContext context, string collection)
        {
            var table = GetTombstoneTableForCollection(context, collection);
            return table?.NumberOfEntries ?? 0;
        }

        public long TombstonesSizeForCollectionInBytes(DocumentsOperationContext context, string collection)
        {
            var table = GetTombstoneTableForCollection(context, collection);
            return table?.GetReport(includeDetails: false).AllocatedSpaceInBytes ?? 0;
        }

        public IEnumerable<Tombstone> GetTombstonesFrom(
            DocumentsOperationContext context,
            string collection,
            long etag,
            long start,
            long take)
        {
            string tableName;

            if (collection == Schemas.Attachments.AttachmentsTombstones ||
                collection == Schemas.Revisions.RevisionsTombstones)
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
                if (tx.LowLevelTransaction.TryGetClientState(out DocumentTransactionCache cache))
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
                if (tx.LowLevelTransaction.TryGetClientState(out DocumentTransactionCache cache))
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

        private bool ReadLastDocument(Transaction transaction, CollectionName collectionName, CollectionTableType collectionType, ref Table.TableValueHolder result)
        {
            var table = transaction.OpenTable(DocumentDatabase.GetDocsSchemaForCollection(collectionName),
                collectionName.GetTableName(collectionType));

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
                if (tx.LowLevelTransaction.TryGetClientState(out DocumentTransactionCache cache))
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
                    Debug.Assert(false, $"Found {assertionKey}({values.Length}) in metadata but {assertionFlag} flag is missing in document {id} (database: {DocumentDatabase.Name}).");
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
                ChangeVector = TableValueToChangeVector(context, (int)TombstoneTable.ChangeVector, ref tvr),
                LastModified = TableValueToDateTime((int)TombstoneTable.LastModified, ref tvr),
                Flags = TableValueToFlags((int)TombstoneTable.Flags, ref tvr)
            };

            switch (result.Type)
            {
                case Tombstone.TombstoneType.Document:
                    result.Collection = TableValueToId(context, (int)TombstoneTable.Collection, ref tvr);
                    result.LowerId = UnwrapLowerIdIfNeeded(context, result.LowerId);
                    break;
                case Tombstone.TombstoneType.Revision:
                    result.Collection = TableValueToId(context, (int)TombstoneTable.Collection, ref tvr);
                    break;
            }

            return result;
        }

        public DeleteOperationResult? Delete(DocumentsOperationContext context, string id, DocumentFlags flags)
        {
            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
            {
                return Delete(context, lowerId, id, expectedChangeVector: null, newFlags: flags);
            }
        }

        public DeleteOperationResult? Delete(DocumentsOperationContext context, string id, string expectedChangeVector, DocumentFlags newFlags = DocumentFlags.None)
        {
            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
            using (var cv = context.GetLazyString(expectedChangeVector))
            {
                return Delete(context, lowerId, id, expectedChangeVector: cv, newFlags: newFlags);
            }
        }

        public DeleteOperationResult? Delete(DocumentsOperationContext context, Slice lowerId, string id,
            LazyStringValue expectedChangeVector, long? lastModifiedTicks = null, ChangeVector changeVector = null,
            CollectionName collectionName = null, NonPersistentDocumentFlags nonPersistentFlags = NonPersistentDocumentFlags.None,
            DocumentFlags newFlags = DocumentFlags.None)
        {
            if (newFlags.HasFlag(DocumentFlags.FromResharding) == false)
                ValidateId(context, lowerId, type: DocumentChangeTypes.Delete, newFlags);

            var fromReplication = nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromReplication);
            var fromResharding = nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromResharding);

            var local = GetDocumentOrTombstone(context, lowerId, throwOnConflict: false);
            var modifiedTicks = GetOrCreateLastModifiedTicks(lastModifiedTicks);

            if (local.Tombstone != null)
            {
                if (expectedChangeVector != null)
                    throw new ConcurrencyException($"Document {local.Tombstone.LowerId} does not exist, but delete was called with change vector '{expectedChangeVector}'. " +
                                                   "Optimistic concurrency violation, transaction will be aborted.")
                    {
                        Id = local.Tombstone.LowerId,
                        ExpectedChangeVector = expectedChangeVector
                    };

                var localCollection = ExtractCollectionName(context, local.Tombstone.Collection);
                if (collectionName == null || local.Tombstone.Collection.Equals(collectionName.Name))
                {
                    collectionName = localCollection;
                }
                else
                {
                    // ensure the table for the tombstones is created
                    ExtractCollectionName(context, collectionName.Name);
                }

                DocumentPut.DeleteTombstoneIfNeeded(context, collectionName, lowerId);

                DocumentFlags flags;
                var localFlags = local.Tombstone.Flags.Strip(DocumentFlags.FromClusterTransaction);
                if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.ByEnforceRevisionConfiguration))
                {
                    //after enforce revision configuration we don't have revision and we want to remove the flag from tombstone
                    flags = localFlags.Strip(DocumentFlags.HasRevisions);
                }
                else
                {
                    flags = localFlags | newFlags;
                    var revisionsStorage = DocumentDatabase.DocumentsStorage.RevisionsStorage;

                    if (fromReplication &&
                        localFlags.Contain(DocumentFlags.HasRevisions) != newFlags.Contain(DocumentFlags.HasRevisions))
                    {
                        var count = revisionsStorage.GetRevisionsCount(context, id);
                        if (count == 0)
                            flags = flags.Strip(DocumentFlags.HasRevisions);
                    }
                    if (collectionName.IsHiLo == false &&
                        (flags & DocumentFlags.Artificial) != DocumentFlags.Artificial)
                    {
                        if (fromReplication == false &&
                            (revisionsStorage.Configuration != null || flags.Contain(DocumentFlags.Resolved)))
                        {
                            revisionsStorage.Delete(context, id, lowerId, collectionName, context.GetChangeVector(changeVector ?? local.Tombstone.ChangeVector),
                                modifiedTicks, nonPersistentFlags, newFlags);
                        }
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
                if (expectedChangeVector != null && ChangeVector.CompareVersion(doc.ChangeVector, expectedChangeVector, context) != 0)
                    ThrowConcurrencyException(id, expectedChangeVector, doc.ChangeVector);

                collectionName = ExtractCollectionName(context, doc.Data);

                var flags = GetFlagsFromOldDocument(newFlags, doc.Flags, nonPersistentFlags);
                var table = context.Transaction.InnerTransaction.OpenTable(DocumentDatabase.GetDocsSchemaForCollection(collectionName, flags), collectionName.GetTableName(CollectionTableType.Documents));

                long etag;
                using (Slice.From(context.Allocator, doc.LowerId, out Slice tombstoneId))
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
                    changeVector = context.GetChangeVector(tombstone.ChangeVector);
                    etag = tombstone.Etag;
                }

                EnsureLastEtagIsPersisted(context, etag);

                var tombstoneChangeVector = context.GetChangeVector(changeVector ?? local.Tombstone?.ChangeVector);
                var revisionsStorage = DocumentDatabase.DocumentsStorage.RevisionsStorage;

                if (fromReplication == false)
                {
                    if (collectionName.IsHiLo == false && flags.Contain(DocumentFlags.Artificial) == false)
                    {
                        var shouldVersion = DocumentDatabase.DocumentsStorage.RevisionsStorage.ShouldVersionDocument(
                            collectionName, nonPersistentFlags, local.Document.Data, null, context, id, lastModifiedTicks, ref flags, out var configuration);

                        if (shouldVersion || flags.Contain(DocumentFlags.HasRevisions))
                        {
                            var localChangeVector = context.GetChangeVector(local.Document.ChangeVector);
                            if (DocumentDatabase.DocumentsStorage.RevisionsStorage.ShouldVersionOldDocument(context, flags, local.Document.Data, localChangeVector, collectionName))
                            {
                                DocumentDatabase.DocumentsStorage.RevisionsStorage.Put(context, id, local.Document.Data, flags | DocumentFlags.HasRevisions | DocumentFlags.FromOldDocumentRevision, NonPersistentDocumentFlags.None,
                                    localChangeVector, local.Document.LastModified.Ticks, configuration, collectionName);
                            }
                            newFlags |= DocumentFlags.HasRevisions;
                            revisionsStorage.Delete(context, id, lowerId, collectionName, tombstoneChangeVector,
                                modifiedTicks, nonPersistentFlags, newFlags);
                        }
                    }

                    if (flags.Contain(DocumentFlags.HasRevisions))
                    {
                        if (revisionsStorage.Configuration == null &&
                            flags.Contain(DocumentFlags.Resolved) == false)
                        {
                            revisionsStorage.DeleteRevisionsFor(context, id, fromDelete: true);
                        }
                    }

                    if (flags.Contain(DocumentFlags.HasAttachments))
                        AttachmentsStorage.DeleteAttachmentsOfDocument(context, lowerId, changeVector, modifiedTicks, newFlags);

                    if (flags.Contain(DocumentFlags.HasCounters))
                        CountersStorage.DeleteCountersForDocument(context, id, collectionName);

                    if (flags.Contain(DocumentFlags.HasTimeSeries))
                        TimeSeriesStorage.DeleteAllTimeSeriesForDocument(context, id, collectionName, flags);
                }
                
                if (fromResharding && revisionsStorage.Configuration != null)
                {
                    revisionsStorage.Delete(context, id, lowerId, collectionName, tombstoneChangeVector,
                        modifiedTicks, nonPersistentFlags, newFlags);
                }

                if (_forTestingPurposes?.DisableDebugAssertionForTableThrowNotOwned == true)
                    table.ForTestingPurposesOnly().DisableDebugAssertionForThrowNotOwned = true;

                table.Delete(doc.StorageId);

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
                    // let's check the conflict storage
                    var collection = ConflictsStorage.GetFirstOrNullCollection(context, id);
                    if (collection == null)
                    {
                        // this basically mean that we tried to delete document that doesn't exist.
                        return null;
                    }

                    collectionName = new CollectionName(collection);
                }

                // ensures that the collection trees will be created
                collectionName = ExtractCollectionName(context, collectionName.Name);

                var etag = CreateTombstone(context,
                    lowerId,
                    GenerateNextEtagForReplicatedTombstoneMissingDocument(context),
                    collectionName,
                    null,
                    modifiedTicks,
                    changeVector,
                    newFlags,
                    nonPersistentFlags).Etag;

                // We've to add notification since we're updating last tombstone etag, and we can end up in situation when our indexes will be stale due unprocessed tombstones after replication.
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
        }

        public virtual void ValidateId(DocumentsOperationContext context, Slice lowerId, DocumentChangeTypes type, DocumentFlags documentFlags = DocumentFlags.None)
        {

        }

        [DoesNotReturn]
        private static void ThrowConcurrencyException(string id, string expected, string actual)
        {
            throw new ConcurrencyException($"Document {id} has change vector {actual}, but Delete was called with change vector '{expected}'. " +
                                           "Optimistic concurrency violation, transaction will be aborted.")
            {
                Id = id,
                ActualChangeVector = actual,
                ExpectedChangeVector = expected
            };
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
        public List<DeleteOperationResult> DeleteDocumentsStartingWith(DocumentsOperationContext context, string prefix, long maxDocsToDelete = long.MaxValue, Action<Document> beforeDeleted = null, DocumentFlags flags = DocumentFlags.None)
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

                    var deleteOperationResult = Delete(context, id, null, flags);
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
            ChangeVector changeVector,
            DocumentFlags flags,
            NonPersistentDocumentFlags nonPersistentFlags)
        {
            var newEtag = GenerateNextEtag();

            if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromReplication))
            {
                flags |= DocumentFlags.FromReplication;
            }
            else
            {
                flags = flags.Strip(DocumentFlags.FromReplication);
            }

            var result = BuildChangeVectorAndResolveConflicts(context, lowerId, newEtag, document: null, changeVector, expectedChangeVector: null, flags,
                oldChangeVector: context.GetChangeVector(docChangeVector));

            if (UpdateLastDatabaseChangeVector(context, result.ChangeVector, flags, nonPersistentFlags))
            {
                changeVector = result.ChangeVector;
            }

            Debug.Assert(changeVector != null, "changeVector can't be null");

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
        public static void FlagsProperlySet(DocumentFlags flags, ChangeVector changeVector)
        {
            CheckFlagsProperlySet(flags, changeVector.Version);
        }

        [Conditional("DEBUG")]
        private static void CheckFlagsProperlySet(DocumentFlags flags, string changeVector)
        {
            var cvArray = changeVector.ToChangeVector();
            var expectedValues = new int[] { ChangeVectorParser.RaftInt, ChangeVectorParser.TrxnInt };
            if (flags.Contain(DocumentFlags.FromClusterTransaction))
            {
                switch (cvArray.Length)
                {
                    case 1:
                        if (cvArray[0].NodeTag != ChangeVectorParser.RaftInt)
                        {
                            Debug.Assert(false, $"FromClusterTransaction, expect RAFT, {changeVector}");
                        }
                        break;
                    case 2:
                        if ((expectedValues.Contains(cvArray[0].NodeTag) == false ||
                            expectedValues.Contains(cvArray[1].NodeTag) == false ||
                            cvArray[0].NodeTag == cvArray[1].NodeTag) &&
                            cvArray[0].NodeTag != ChangeVectorParser.SinkInt &&
                            cvArray[1].NodeTag != ChangeVectorParser.SinkInt)
                        {
                            Debug.Assert(false, $"FromClusterTransaction, expect RAFT or TRXN or SINK, {changeVector}");
                        }
                        break;
                    default:
                        Debug.Assert(false, $"FromClusterTransaction, expect change vector of length 1 or 2, {changeVector}");
                        break;
                }
            }

            switch (cvArray.Length)
            {
                case 1:
                    if (cvArray[0].NodeTag == ChangeVectorParser.RaftInt)
                    {
                        if (flags.Contain(DocumentFlags.FromClusterTransaction) == false)
                        {
                            Debug.Assert(false, $"flags must set FromClusterTransaction for the change vector: {changeVector}");
                        }
                    }
                    break;
                case 2:
                    if (expectedValues.Contains(cvArray[0].NodeTag) && expectedValues.Contains(cvArray[1].NodeTag))
                    {
                        if (flags.Contain(DocumentFlags.FromClusterTransaction) == false)
                        {
                            Debug.Assert(false, $"flags: '{flags}' must set FromClusterTransaction for the change vector: {changeVector}");
                        }
                    }
                    break;
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
            if (NeedToUnwrapLowerId(lowerId.Buffer, lowerId.Size) == false)
                return lowerId;

            var size = lowerId.Size - ConflictedTombstoneOverhead;
            var allocated = context.GetMemory(size + 1); // we need this extra byte to mark that there is no escaping
            allocated.Address[size] = 0;
            Memory.Copy(allocated.Address, lowerId.Buffer, size);
            var lsv = context.AllocateStringValue(null, allocated.Address, size);
            lsv.AllocatedMemoryData = allocated;
            return lsv;
        }

        protected static int GetSizeOfTombstoneId(byte* lowerId, int size)
        {
            if (NeedToUnwrapLowerId(lowerId, size) == false)
                return size;

            return size - ConflictedTombstoneOverhead;
        }

        private static bool NeedToUnwrapLowerId(byte* lowerId, int size)
        {
            if (size < ConflictedTombstoneOverhead + 1)
                return false;

            if (lowerId[size - ConflictedTombstoneOverhead] != SpecialChars.RecordSeparator)
                return false;

            return true;
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

        [DoesNotReturn]
        private void ThrowNotSupportedExceptionForCreatingTombstoneWhenItExistsForDifferentCollection(Slice lowerId, CollectionName collectionName,
            CollectionName tombstoneCollectionName, VoronConcurrencyErrorException e)
        {
            var tombstoneCleanerState = DocumentDatabase.TombstoneCleaner.GetState().Tombstones;
            if (tombstoneCleanerState.TryGetValue(tombstoneCollectionName.Name, out var item) && item.Documents.Component != null)
                throw new NotSupportedException($"Could not delete document '{lowerId}' from collection '{collectionName.Name}' because tombstone for that document already exists but in a different collection ('{tombstoneCollectionName.Name}'). Did you change the document's collection recently? If yes, please give some time for other system components (e.g. Indexing, Replication, Backup) and tombstone cleaner to process that change. At this point of time the component that holds the tombstone is '{item.Documents.Component}' with etag '{item.Documents.Etag}' and tombstone cleaner is executed every '{DocumentDatabase.Configuration.Tombstones.CleanupInterval.AsTimeSpan.TotalMinutes}' minutes.", e);

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
                tx.OpenTable(DocumentDatabase.GetDocsSchemaForCollection(collectionObject), collectionName);

            table.Delete(storageId);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PutOperationResults Put(DocumentsOperationContext context, string id,
            string expectedChangeVector, BlittableJsonReaderObject document, long? lastModifiedTicks = null, string changeVector = null,
            string oldChangeVectorForClusterTransactionIndexCheck = null,
            DocumentFlags flags = DocumentFlags.None, NonPersistentDocumentFlags nonPersistentFlags = NonPersistentDocumentFlags.None)
        {
            ChangeVector cv = null;
            if (changeVector != null)
                cv = context.GetChangeVector(changeVector);

            return DocumentPut.PutDocument(context, id, expectedChangeVector, document, lastModifiedTicks, cv, oldChangeVectorForClusterTransactionIndexCheck, flags, nonPersistentFlags);
        }

        public long GetNumberOfDocumentsToProcess(DocumentsOperationContext context, string collection, long afterEtag, out long totalCount, Stopwatch overallDuration)
        {
            return GetNumberOfItemsToProcess(context, collection, afterEtag, tombstones: false, totalCount: out totalCount, overallDuration);
        }

        public long GetNumberOfTombstonesToProcess(DocumentsOperationContext context, string collection, long afterEtag, out long totalCount, Stopwatch overallDuration)
        {
            return GetNumberOfItemsToProcess(context, collection, afterEtag, tombstones: true, totalCount: out totalCount, overallDuration);
        }

        private long GetNumberOfItemsToProcess(DocumentsOperationContext context, string collection, long afterEtag, bool tombstones, out long totalCount,
            Stopwatch overallDuration)
        {
            var collectionName = GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
            {
                totalCount = 0;
                return 0;
            }

            Table table;
            TableSchema.FixedSizeKeyIndexDef indexDef;
            if (tombstones)
            {
                table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema,
                    collectionName.GetTableName(CollectionTableType.Tombstones));

                indexDef = TombstonesSchema.FixedSizeIndexes[CollectionEtagsSlice];
            }
            else
            {
                table = context.Transaction.InnerTransaction.OpenTable(DocumentDatabase.GetDocsSchemaForCollection(collectionName),
                    collectionName.GetTableName(CollectionTableType.Documents));
                indexDef = DocsSchema.FixedSizeIndexes[CollectionEtagsSlice];
            }

            if (table == null)
            {
                totalCount = 0;
                return 0;
            }

            return table.GetNumberOfEntriesAfter(indexDef, afterEtag, out totalCount, overallDuration);
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

        public sealed class CollectionStats
        {
            public string Name;
            public long Count;
        }

        public IEnumerable<CollectionStats> GetCollections(DocumentsOperationContext context)
        {
            foreach (var kvp in _collectionsCache)
            {
                var collectionTable = context.Transaction.InnerTransaction.OpenTable(DocumentDatabase.GetDocsSchemaForCollection(kvp.Value),
                    kvp.Value.GetTableName(CollectionTableType.Documents));
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
            CollectionDetails collectionDetails = new CollectionDetails
            {
                Name = collection,
                CountOfDocuments = 0,
                Size = new Client.Util.Size(),
                DocumentsSize = new Client.Util.Size(),
                RevisionsSize = new Client.Util.Size(),
                TombstonesSize = new Client.Util.Size()
            };
            CollectionName collectionName = GetCollection(collection, throwIfDoesNotExist: false);

            if (collectionName != null)
            {
                TableReport collectionTableReport = GetReportForTable(context, DocsSchema, collectionName.GetTableName(CollectionTableType.Documents));

                collectionDetails.CountOfDocuments = collectionTableReport.NumberOfEntries;

                var documentsSize = collectionTableReport.DataSizeInBytes;
                var revisionsSize = GetReportForTable(context, RevisionsStorage.RevisionsSchema, collectionName.GetTableName(CollectionTableType.Revisions))
                    .DataSizeInBytes;
                var tombstonesSize = GetReportForTable(context, TombstonesSchema, collectionName.GetTableName(CollectionTableType.Tombstones)).DataSizeInBytes;

                collectionDetails.DocumentsSize.SizeInBytes = documentsSize;
                collectionDetails.RevisionsSize.SizeInBytes = revisionsSize;
                collectionDetails.TombstonesSize.SizeInBytes = tombstonesSize;

                collectionDetails.Size.SizeInBytes = documentsSize + revisionsSize + tombstonesSize;
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

            var collectionTable = context.Transaction.InnerTransaction.OpenTable(DocumentDatabase.GetDocsSchemaForCollection(collectionName),
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

            if (collection == Schemas.Attachments.AttachmentsTombstones ||
                collection == Schemas.Revisions.RevisionsTombstones)
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
            yield return Schemas.Attachments.AttachmentsTombstones;
            yield return Schemas.Revisions.RevisionsTombstones;

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

        public ConflictStatus GetConflictStatus(DocumentsOperationContext context, string remote, string local, ChangeVectorMode mode) => GetConflictStatus(context, remote, local, mode, out _);

        public ConflictStatus GetConflictStatus(DocumentsOperationContext context, string remote, string local, ChangeVectorMode mode, out bool skipValidation)
        {
            var remoteChangeVector = context.GetChangeVector(remote);
            var localChangeVector = context.GetChangeVector(local);

            skipValidation = false;
            var originalStatus = ChangeVectorUtils.GetConflictStatus(remoteChangeVector, localChangeVector, mode: mode);
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

                var original = ChangeVectorUtils.GetConflictStatus(remoteChangeVector, localChangeVector, mode: mode);

                remoteChangeVector = remoteChangeVector.StripTrxnTags(context);
                localChangeVector = localChangeVector.StripTrxnTags(context);

                remoteChangeVector.TryRemoveIds(UnusedDatabaseIds, context, out remoteChangeVector);
                skipValidation = localChangeVector.TryRemoveIds(UnusedDatabaseIds, context, out localChangeVector);
                var after = ChangeVectorUtils.GetConflictStatus(remoteChangeVector, localChangeVector, mode: mode);

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
                    var collectionNames = new Dictionary<string, CollectionName>(_collectionsCache, StringComparer.OrdinalIgnoreCase)
                    {
                        [name.Name] = name
                    };
                    _collectionsCache = collectionNames;
                };
            }
            return name;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (ChangeVector ChangeVector, NonPersistentDocumentFlags NonPersistentFlags) BuildChangeVectorAndResolveConflicts(
            DocumentsOperationContext context, Slice lowerId, long newEtag,
            BlittableJsonReaderObject document, ChangeVector changeVector, string expectedChangeVector, DocumentFlags flags, ChangeVector oldChangeVector)
        {
            var nonPersistentFlags = NonPersistentDocumentFlags.None;
            var fromReplication = flags.Contain(DocumentFlags.FromReplication);

            if (ConflictsStorage.NumberOfConflicts(context) != 0)
            {
                // Since this document resolve the conflict we don't need to alter the change vector.
                // This way we avoid another replication back to the source

                ConflictsStorage.ThrowConcurrencyExceptionOnConflictIfNeeded(context, lowerId, expectedChangeVector);

                if (fromReplication)
                {
                    nonPersistentFlags = ConflictsStorage.DeleteConflictsFor(context, lowerId, document).NonPersistentFlags;
                }
                else
                {
                    (changeVector, nonPersistentFlags) = ConflictsStorage.MergeConflictChangeVectorIfNeededAndDeleteConflicts(changeVector, context, lowerId, newEtag, document);
                }
            }

            if (changeVector != null)
                return (changeVector, nonPersistentFlags);

            if (fromReplication == false)
            {
                oldChangeVector = ChangeVector.MergeWithDatabaseChangeVector(context, oldChangeVector);
            }

            changeVector = SetDocumentChangeVectorForLocalChange(context, lowerId, oldChangeVector, newEtag);
            context.SkipChangeVectorValidation = changeVector.TryRemoveIds(UnusedDatabaseIds, context, out changeVector);
            return (changeVector, nonPersistentFlags);
        }

        public static bool UpdateLastDatabaseChangeVector(DocumentsOperationContext context, ChangeVector changeVector, DocumentFlags flags, NonPersistentDocumentFlags nonPersistentFlags)
        {
            // if arrived from replication we keep the document with its original change vector
            // in that case the updating of the global change vector should happened upper in the stack
            if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromReplication))
                return false;

            var currentGlobalChangeVector = context.LastDatabaseChangeVector ?? GetDatabaseChangeVector(context);

            var clone = context.GetChangeVector(changeVector);
            clone = clone.StripSinkTags(currentGlobalChangeVector, context);

            // this is raft created document, so it must contain only the RAFT element 
            if (flags.Contain(DocumentFlags.FromClusterTransaction))
            {
                context.LastDatabaseChangeVector = ChangeVector.Merge(currentGlobalChangeVector, clone.Order, context);
                return false;
            }

            // the resolved document must preserve the original change vector (without the global change vector) to avoid ping-pong replication.
            if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromResolver))
            {
                context.LastDatabaseChangeVector = ChangeVector.Merge(currentGlobalChangeVector, clone.Order, context);
                return false;
            }

            context.LastDatabaseChangeVector = clone.Order;
            return true;
        }

        private ChangeVector SetDocumentChangeVectorForLocalChange(DocumentsOperationContext context, Slice lowerId, ChangeVector oldChangeVector, long newEtag)
        {
            if (oldChangeVector != null)
            {
                oldChangeVector = oldChangeVector.UpdateVersion(DocumentDatabase.ServerStore.NodeTag, Environment.Base64Id, newEtag, context);
                oldChangeVector = oldChangeVector.UpdateOrder(DocumentDatabase.ServerStore.NodeTag, Environment.Base64Id, newEtag, context);
                return oldChangeVector;
            }
            return ConflictsStorage.GetMergedConflictChangeVectorsAndDeleteConflicts(context, lowerId, newEtag);
        }

        public DocumentFlags GetFlagsFromOldDocument(DocumentFlags newFlags, DocumentFlags oldFlags, NonPersistentDocumentFlags nonPersistentFlags)
        {
            if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromReplication))
                return newFlags;

            newFlags = newFlags.Strip(DocumentFlags.FromReplication);

            if (newFlags.Contain(DocumentFlags.Reverted))
            {
                // we set the flags in the caller, because we might revert not to _oldFlags_ but to something prior to that
                return newFlags;
            }

            if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.Resolved))
            {
                newFlags |= DocumentFlags.Resolved;
            }

            if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.ByAttachmentUpdate) == false &&
                oldFlags.Contain(DocumentFlags.HasAttachments))
            {
                newFlags |= DocumentFlags.HasAttachments;
            }

            if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.ByCountersUpdate) == false &&
                oldFlags.Contain(DocumentFlags.HasCounters))
            {
                newFlags |= DocumentFlags.HasCounters;
            }

            if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.ByTimeSeriesUpdate) == false &&
                oldFlags.Contain(DocumentFlags.HasTimeSeries))
            {
                newFlags |= DocumentFlags.HasTimeSeries;
            }

            if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.ByEnforceRevisionConfiguration) == false &&
                oldFlags.Contain(DocumentFlags.HasRevisions))
            {
                newFlags |= DocumentFlags.HasRevisions;
            }

            return newFlags;
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

        [DoesNotReturn]
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
            var result = new Dictionary<string, CollectionName>(StringComparer.OrdinalIgnoreCase);

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

        public static Dictionary<string, CollectionName> ReadCollections(Transaction tx, JsonOperationContext context)
        {
            var result = new Dictionary<string, CollectionName>(StringComparer.OrdinalIgnoreCase);
            var collections = tx.OpenTable(CollectionsSchema, CollectionsSlice);
            foreach (var tvr in collections.SeekByPrimaryKey(Slices.BeforeAllKeys, 0))
            {
                var collection = TableValueToId(context, (int)CollectionsTable.Name, ref tvr.Reader);
                var collectionName = new CollectionName(collection);
                result.Add(collection, collectionName);
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

        [DoesNotReturn]
        private static void ThrowInvalidTagLength()
        {
            throw new InvalidOperationException($"The tag length is invalid.");
        }

        [DoesNotReturn]
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
        public static ChangeVector TableValueToChangeVector(DocumentsOperationContext context, int index, ref TableValueReader tvr)
        {
            var ptr = tvr.Read(index, out int size);
            return context.GetChangeVector(Encodings.Utf8.GetString(ptr, size));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static LazyStringValue TableValueToId(JsonOperationContext context, int index, ref TableValueReader tvr)
        {
            var ptr = tvr.Read(index, out _);
            var lzs = context.GetLazyStringValue(ptr, out bool success);
            if (success == false)
                ThrowInvalidTagLength();
            return lzs;
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
