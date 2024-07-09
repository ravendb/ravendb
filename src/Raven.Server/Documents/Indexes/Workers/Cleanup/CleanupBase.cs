using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.Documents.Indexes.Workers.Cleanup
{
    public abstract class CleanupBase : IIndexingWork
    {
        private readonly Logger _logger;

        private readonly Index _index;
        private readonly IndexItemType _itemType;
        private readonly IndexingConfiguration _configuration;
        private readonly IndexStorage _indexStorage;
        private readonly MapReduceIndexingContext _mapReduceContext;

        protected CleanupBase(Index index, IndexStorage indexStorage, IndexingConfiguration configuration, MapReduceIndexingContext mapReduceContext)
        {
            _index = index;
            _itemType = _index.ItemType;
            _configuration = configuration;
            _mapReduceContext = mapReduceContext;
            _indexStorage = indexStorage;
            _logger = LoggingSource.Instance
                .GetLogger<CleanupBase>(indexStorage.DocumentDatabase.Name);
        }

        public string Name => "Cleanup";

        protected abstract IEnumerable<TombstoneIndexItem> GetTombstonesFrom(DocumentsOperationContext context, long etag, long start, long take);

        protected abstract IEnumerable<TombstoneIndexItem> GetTombstonesFrom(DocumentsOperationContext context, string collection, long etag, long start, long take);

        protected abstract bool ValidateType(TombstoneIndexItem tombstone);

        protected abstract bool HandleDelete(TombstoneIndexItem tombstone, string collection, Lazy<IndexWriteOperationBase> writer, QueryOperationContext queryContext,
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
                    var lastMappedEtag = _indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection);
                    var lastTombstoneEtag = _indexStorage.ReadLastProcessedTombstoneEtag(_itemType, indexContext.Transaction, collection);

                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Executing cleanup for '{_index} ({_index.Name})'. Collection: {collection}. LastMappedEtag: {lastMappedEtag:#,#;;0}. LastTombstoneEtag: {lastTombstoneEtag:#,#;;0}.");

                    var inMemoryStats = _index.GetStats(collection);
                    var lastEtag = lastTombstoneEtag;
                    var count = 0;

                    var sw = new Stopwatch();
                    var keepRunning = true;
                    var lastCollectionEtag = -1L;
                    while (keepRunning)
                    {
                        var hasChanges = false;
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
                                inMemoryStats.UpdateLastEtag(lastEtag, isTombstone: true, isTimeSeriesDeletedRange: _index.ItemType == IndexItemType.TimeSeries);

                                if (_logger.IsInfoEnabled && totalProcessedCount % 2048 == 0)
                                    _logger.Info($"Executing cleanup for '{_index.Name}'. Processed count: {totalProcessedCount:#,#;;0} etag: {lastEtag}.");

                                if (ValidateType(tombstone) == false)
                                    continue; // this can happen when we have '@all_docs'

                                var deleted = HandleDelete(tombstone, collection, writeOperation, queryContext, indexContext, collectionStats);
                                stats.RecordTombstoneDeleteSuccess();

                                if (deleted && _itemType == IndexItemType.TimeSeries)
                                    break;

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

                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Executing cleanup for '{_index} ({_index.Name})'. Processed {count} tombstones in '{collection}' collection in {collectionStats.Duration.TotalMilliseconds:#,#;;0} ms.");

                    if (_index.Type.IsMap())
                    {
                        _indexStorage.WriteLastTombstoneEtag(_itemType, indexContext.Transaction, collection, lastEtag);
                    }
                    else
                    {
                        if (_itemType == IndexItemType.TimeSeries)
                            _mapReduceContext.ProcessedTimeSeriesDeletedRangeEtags[collection] = lastEtag;
                        else
                            _mapReduceContext.ProcessedTombstoneEtags[collection] = lastEtag;
                    }

                    moreWorkFound = true;
                }
            }

            return (moreWorkFound, batchContinuationResult);
        }
    }
}
