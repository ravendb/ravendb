using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;

namespace Raven.Server.Documents.Indexes.Workers
{
    public abstract class MapItems : IIndexingWork
    {
        private readonly RavenLogger _logger;
        private readonly Index _index;
        private readonly MapReduceIndexingContext _mapReduceContext;
        private readonly IndexingConfiguration _configuration;
        private readonly IndexStorage _indexStorage;

        protected MapItems(Index index, IndexStorage indexStorage, MapReduceIndexingContext mapReduceContext, IndexingConfiguration configuration)
        {
            _index = index;
            _mapReduceContext = mapReduceContext;
            _configuration = configuration;
            _indexStorage = indexStorage;
            _logger = RavenLogManager.Instance.GetLoggerForIndex(GetType(), index);
        }

        public string Name => "Map";

        public (bool MoreWorkFound, Index.CanContinueBatchResult BatchContinuationResult) Execute(QueryOperationContext queryContext, TransactionOperationContext indexContext,
            Lazy<IndexWriteOperationBase> writeOperation, IndexingStatsScope stats, CancellationToken token)
        {
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

                    if (_logger.IsDebugEnabled)
                        _logger.Debug($"Executing map for '{_index.Name}'. Collection: {collection} LastMappedEtag: {lastMappedEtag:#,#;;0}.");

                    var inMemoryStats = _index.GetStats(collection);
                    var lastEtag = lastMappedEtag;
                    var resultsCount = 0;
                    var pageSize = int.MaxValue;

                    var sw = new Stopwatch();
                    var keepRunning = true;
                    var lastCollectionEtag = -1L;
                    while (keepRunning)
                    {
                        batchContinuationResult = Index.CanContinueBatchResult.None;

                        using (queryContext.OpenReadTransaction())
                        {
                            sw.Restart();

                            if (lastCollectionEtag == -1)
                                lastCollectionEtag = _index.GetLastItemEtagInCollection(queryContext, collection);

                            var items = GetItemsEnumerator(queryContext, collection, lastEtag, pageSize);

                            if (_index.TestRun != null)
                                items = _index.TestRun.CreateEnumeratorWrapper(items, collection);
                            
                            using (var itemEnumerator = _index.GetMapEnumerator(items, collection, indexContext, collectionStats, _index.Type))
                            {
                                while (true)
                                {
                                    var prevEtag = lastEtag;

                                    if (itemEnumerator.MoveNext(queryContext.Documents, out IEnumerable mapResults, out var etag) == false)
                                    {
                                        if (etag > lastEtag)
                                            lastEtag = etag.Value;

                                        collectionStats.RecordBatchCompletedReason(IndexingWorkType.Map, "No more documents to index");
                                        keepRunning = false;
                                        break;
                                    }

                                    token.ThrowIfCancellationRequested();

                                    var current = itemEnumerator.Current;

                                    // we cannot break the indexing batch we are in the same etag batch
                                    // this can happen for counters, because we are processing counters from counter group separately
                                    if (prevEtag != current.Etag)
                                    {
                                        if (batchContinuationResult == Index.CanContinueBatchResult.False)
                                        {
                                            if (_index.TestRun != null)
                                                _index.TestRun.HandleCanContinueBatch(batchContinuationResult, collection);

                                            keepRunning = false;
                                            break;
                                        }

                                        if (batchContinuationResult == Index.CanContinueBatchResult.RenewTransaction)
                                        {
                                            if (_index.TestRun != null)
                                                _index.TestRun.HandleCanContinueBatch(batchContinuationResult, collection);

                                            break;
                                        }

                                        if (totalProcessedCount >= pageSize)
                                        {
                                            keepRunning = false;
                                            break;
                                        }
                                    }

                                    totalProcessedCount++;
                                    collectionStats.RecordMapAttempt();
                                    stats.RecordDocumentSize(current.Size);
                                    if (_logger.IsDebugEnabled && totalProcessedCount % 8192 == 0)
                                        _logger.Debug($"Executing map for '{_index.Name}'. Processed count: {totalProcessedCount:#,#;;0} etag: {lastEtag:#,#;;0}.");

                                    lastEtag = current.Etag;
                                    inMemoryStats.UpdateLastEtag(lastEtag, isTombstone: false);

                                    try
                                    {
                                        current.KnownToBeNew = _indexStorage.LowerThanLastDatabaseEtagOnIndexCreation(current.Etag);

                                        if (itemEnumerator.Current.ShouldBeProcessedAsArchived(_index.ArchivedDataProcessingBehavior) == false)
                                        {
                                            var numberOfResults = _index.HandleMap(current, mapResults,
                                                writeOperation, indexContext, collectionStats);
                                            
                                            resultsCount += numberOfResults;
                                            
                                            collectionStats.RecordMapSuccess();
                                            _index.MapsPerSec?.MarkSingleThreaded(numberOfResults);
                                        }
                                        else
                                        {   // skip from indexing
                                            _index.DeleteArchived(current, collection, writeOperation, indexContext, stats, itemEnumerator.Current.LowerId);
                                        }
                                    }
                                    catch (Exception e) when (e.IsIndexError())
                                    {
                                        itemEnumerator.OnError();
                                        _index.ErrorIndexIfCriticalException(e);

                                        collectionStats.RecordMapError();
                                        if (_logger.IsInfoEnabled)
                                            _logger.Info($"Failed to execute mapping function on '{current.Id}' for '{_index.Name}'.", e);

                                        collectionStats.AddMapError(current.Id, $"Failed to execute mapping function on {current.Id}. " +
                                                                                $"Exception: {e}");
                                    }

                                    var parameters = new CanContinueBatchParameters(collectionStats, IndexingWorkType.Map, queryContext, indexContext, writeOperation,
                                        lastEtag, lastCollectionEtag, totalProcessedCount, sw);

                                    batchContinuationResult = _index.CanContinueBatch(in parameters, ref maxTimeForDocumentTransactionToRemainOpen);
                                }
                            }
                        }
                    }

                    if (lastMappedEtag == lastEtag)
                    {
                        // the last mapped etag hasn't changed
                        continue;
                    }

                    moreWorkFound = true;

                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Executed map for '{_index.Name}' index and '{collection}' collection. Got {resultsCount:#,#;;0} map results in {collectionStats.Duration.TotalMilliseconds:#,#;;0} ms.");

                    if (_index.Type.IsMap())
                    {
                        _index.SaveLastState();
                        _indexStorage.WriteLastIndexedEtag(indexContext.Transaction, collection, lastEtag);
                    }
                    else
                    {
                        _mapReduceContext.ProcessedDocEtags[collection] = lastEtag;
                    }
                }
            }

            return (moreWorkFound, batchContinuationResult);
        }

        protected abstract IEnumerable<IndexItem> GetItemsEnumerator(QueryOperationContext queryContext, string collection, long lastEtag, long pageSize);
    }
}
