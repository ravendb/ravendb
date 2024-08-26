using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.Documents.Indexes.Workers.Cleanup
{
    public abstract class CleanupItemsBase : IIndexingWork
    {
        private readonly RavenLogger _logger;

        private readonly Index _index;
        private readonly IndexingConfiguration _configuration;

        protected readonly IndexStorage IndexStorage;

        protected CleanupItemsBase(Index index, IndexStorage indexStorage, IndexingConfiguration configuration, MapReduceIndexingContext mapReduceContext)
        {
            _index = index;
            _configuration = configuration;
            _logger = RavenLogManager.Instance.GetLoggerForIndex<CleanupDocuments>(index);

            IndexStorage = indexStorage;
        }

        public abstract string Name { get; }

        protected abstract long ReadLastProcessedTombstoneEtag(RavenTransaction transaction, string collection);

        protected abstract void WriteLastProcessedTombstoneEtag(RavenTransaction transaction, string collection, long lastEtag);

        internal abstract void UpdateStats(IndexProgress.CollectionStats inMemoryStats, long lastEtag);

        protected abstract IEnumerable<TombstoneIndexItem> GetTombstonesFrom(DocumentsOperationContext context, long etag, long start, long take);

        protected abstract IEnumerable<TombstoneIndexItem> GetTombstonesFrom(DocumentsOperationContext context, string collection, long etag, long start, long take);

        protected abstract bool IsValidTombstoneType(TombstoneIndexItem tombstone);

        protected abstract void HandleDelete(TombstoneIndexItem tombstone, string collection, Lazy<IndexWriteOperationBase> writer, QueryOperationContext queryContext,
            TransactionOperationContext indexContext, IndexingStatsScope stats);


        public virtual (bool MoreWorkFound, Index.CanContinueBatchResult BatchContinuationResult) Execute(QueryOperationContext queryContext, TransactionOperationContext indexContext,
            Lazy<IndexWriteOperationBase> writeOperation, IndexingStatsScope stats, CancellationToken token)
        {
            const long pageSize = long.MaxValue;
            var maxTimeForDocumentTransactionToRemainOpen = Debugger.IsAttached == false
                ? _configuration.MaxTimeForDocumentTransactionToRemainOpen.AsTimeSpan
                : TimeSpan.FromMinutes(15);

            var moreWorkFound = false;
            var batchContinuationResult = Index.CanContinueBatchResult.None;
            var totalProcessedCount = 0;

            foreach (var collection in _index.Collections)
            {
                using (var collectionStats = stats.For("Collection_" + collection))
                {
                    var lastMappedEtag = IndexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection);
                    var lastTombstoneEtag = ReadLastProcessedTombstoneEtag(indexContext.Transaction, collection);

                    if (_logger.IsDebugEnabled)
                        _logger.Debug($"Executing cleanup for '{_index} ({_index.Name})'. Collection: {collection}. LastMappedEtag: {lastMappedEtag:#,#;;0}. LastTombstoneEtag: {lastTombstoneEtag:#,#;;0}.");

                    var inMemoryStats = _index.GetStats(collection);
                    var lastEtag = lastTombstoneEtag;
                    var count = 0;

                    var sw = new Stopwatch();
                    var keepRunning = true;
                    var lastCollectionEtag = -1L;
                    while (keepRunning)
                    {
                        var hasChanges = false;

                        ClearStatsIfNeeded();

                        using (queryContext.OpenReadTransaction())
                        {
                            sw.Restart();

                            if (lastCollectionEtag == -1)
                                lastCollectionEtag = _index.GetLastTombstoneEtagInCollection(queryContext, collection);

                            var tombstones = collection == Constants.Documents.Collections.AllDocumentsCollection
                                ? GetTombstonesFrom(queryContext.Documents, lastEtag + 1, 0, pageSize)
                                : GetTombstonesFrom(queryContext.Documents, collection, lastEtag + 1, 0, pageSize);

                            foreach (var tombstone in tombstones)
                            {
                                token.ThrowIfCancellationRequested();

                                count++;
                                totalProcessedCount++;
                                hasChanges = true;
                                lastEtag = tombstone.Etag;
                                UpdateStats(inMemoryStats, lastEtag);

                                if (_logger.IsDebugEnabled && totalProcessedCount % 2048 == 0)
                                    _logger.Debug($"Executing cleanup for '{_index.Name}'. Processed count: {totalProcessedCount:#,#;;0} etag: {lastEtag}.");

                                if (IsValidTombstoneType(tombstone) == false)
                                    continue; // this can happen when we have '@all_docs'

                                HandleDelete(tombstone, collection, writeOperation, queryContext, indexContext, collectionStats);
                                stats.RecordTombstoneDeleteSuccess();

                                var parameters = new CanContinueBatchParameters(stats, IndexingWorkType.Cleanup, queryContext, indexContext, writeOperation, lastEtag,
                                    lastCollectionEtag,
                                    totalProcessedCount, sw);

                                batchContinuationResult = _index.CanContinueBatch(in parameters, ref maxTimeForDocumentTransactionToRemainOpen);

                                if (batchContinuationResult != Index.CanContinueBatchResult.True)
                                {
                                    keepRunning = batchContinuationResult == Index.CanContinueBatchResult.RenewTransaction;
                                    break;
                                }
                            }

                            if (hasChanges == false)
                                break;
                        }
                    }

                    if (count == 0)
                        continue;

                    if (_logger.IsDebugEnabled)
                        _logger.Debug($"Executing cleanup for '{_index} ({_index.Name})'. Processed {count} tombstones in '{collection}' collection in {collectionStats.Duration.TotalMilliseconds:#,#;;0} ms.");

                    WriteLastProcessedTombstoneEtag(indexContext.Transaction, collection, lastEtag);

                    moreWorkFound = true;
                }
            }

            return (moreWorkFound, batchContinuationResult);
        }

        protected virtual void ClearStatsIfNeeded()
        {
    }
}
}
