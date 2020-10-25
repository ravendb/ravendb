using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;
using Voron;

namespace Raven.Server.Documents.Indexes.Workers
{
    public abstract class MapItems : IIndexingWork
    {
        private readonly Logger _logger;
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
            _logger = LoggingSource.Instance
                .GetLogger<MapDocuments>(indexStorage.DocumentDatabase.Name);
        }

        public string Name => "Map";

        public bool Execute(QueryOperationContext queryContext, TransactionOperationContext indexContext,
            Lazy<IndexWriteOperation> writeOperation, IndexingStatsScope stats, CancellationToken token)
        {
            var maxTimeForDocumentTransactionToRemainOpen = Debugger.IsAttached == false
                ? _configuration.MaxTimeForDocumentTransactionToRemainOpen.AsTimeSpan
                : TimeSpan.FromMinutes(15);

            var moreWorkFound = false;
            var totalProcessedCount = 0;
            foreach (var collection in _index.Collections)
            {
                using (var collectionStats = stats.For("Collection_" + collection))
                {
                    var lastMappedEtag = _indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection);

                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Executing map for '{_index.Name}'. Collection: {collection} LastMappedEtag: {lastMappedEtag:#,#;;0}.");

                    var inMemoryStats = _index.GetStats(collection);
                    var lastEtag = lastMappedEtag;
                    var resultsCount = 0;
                    var pageSize = int.MaxValue;

                    var sw = new Stopwatch();
                    IndexWriteOperation indexWriter = null;
                    var keepRunning = true;
                    var lastCollectionEtag = -1L;
                    while (keepRunning)
                    {
                        using (queryContext.OpenReadTransaction())
                        {
                            sw.Restart();

                            if (lastCollectionEtag == -1)
                                lastCollectionEtag = _index.GetLastItemEtagInCollection(queryContext, collection);

                            var items = GetItemsEnumerator(queryContext, collection, lastEtag, pageSize);

                            using (var itemEnumerator = _index.GetMapEnumerator(items, collection, indexContext, collectionStats, _index.Type))
                            {
                                while (true)
                                {
                                    if (itemEnumerator.MoveNext(queryContext.Documents, out IEnumerable mapResults, out var etag) == false)
                                    {
                                        if (etag > lastEtag)
                                            lastEtag = etag.Value;

                                        collectionStats.RecordMapCompletedReason("No more documents to index");
                                        keepRunning = false;
                                        break;
                                    }

                                    token.ThrowIfCancellationRequested();

                                    if (indexWriter == null)
                                        indexWriter = writeOperation.Value;

                                    var current = itemEnumerator.Current;

                                    totalProcessedCount++;
                                    collectionStats.RecordMapAttempt();
                                    stats.RecordDocumentSize(current.Size);
                                    if (_logger.IsInfoEnabled && totalProcessedCount % 8192 == 0)
                                        _logger.Info($"Executing map for '{_index.Name}'. Processed count: {totalProcessedCount:#,#;;0} etag: {lastEtag:#,#;;0}.");

                                    lastEtag = current.Etag;
                                    inMemoryStats.UpdateLastEtag(lastEtag, isTombstone: false);

                                    try
                                    {
                                        var numberOfResults = _index.HandleMap(current, mapResults,
                                            indexWriter, indexContext, collectionStats);

                                        resultsCount += numberOfResults;
                                        collectionStats.RecordMapSuccess();
                                        _index.MapsPerSec?.MarkSingleThreaded(numberOfResults);
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

                                    if (CanContinueBatch(queryContext, indexContext, collectionStats, indexWriter, lastEtag, lastCollectionEtag, totalProcessedCount) == false)
                                    {
                                        keepRunning = false;
                                        break;
                                    }

                                    if (totalProcessedCount >= pageSize)
                                    {
                                        keepRunning = false;
                                        break;
                                    }

                                    if (MaybeRenewTransaction(queryContext, sw, _configuration, ref maxTimeForDocumentTransactionToRemainOpen))
                                        break;
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

            return moreWorkFound;
        }

        public static bool MaybeRenewTransaction(
            QueryOperationContext queryContext, Stopwatch sw,
            IndexingConfiguration configuration,
            ref TimeSpan maxTimeForDocumentTransactionToRemainOpen)
        {
            if (sw.Elapsed > maxTimeForDocumentTransactionToRemainOpen)
            {
                if (queryContext.Documents.ShouldRenewTransactionsToAllowFlushing())
                    return true;

                // if we haven't had writes in the meantime, there is no point
                // in replacing the database transaction, and it will probably cost more
                // let us check again later to see if we need to
                maxTimeForDocumentTransactionToRemainOpen =
                    maxTimeForDocumentTransactionToRemainOpen.Add(
                        configuration.MaxTimeForDocumentTransactionToRemainOpen.AsTimeSpan);
            }
            return false;
        }

        public bool CanContinueBatch(QueryOperationContext queryContext, TransactionOperationContext indexingContext, IndexingStatsScope stats, IndexWriteOperation indexWriter, long currentEtag, long maxEtag, long count)
        {
            if (_index.ShouldRunCanContinueBatch(count) == false)
                return true;

            if (stats.Duration >= _configuration.MapTimeout.AsTimeSpan)
            {
                stats.RecordMapCompletedReason($"Exceeded maximum configured map duration ({_configuration.MapTimeout.AsTimeSpan}). Was {stats.Duration}");
                return false;
            }

            if (_configuration.MapBatchSize.HasValue && count >= _configuration.MapBatchSize.Value)
            {
                stats.RecordMapCompletedReason($"Reached maximum configured map batch size ({_configuration.MapBatchSize.Value:#,#;;0}).");
                return false;
            }

            if (currentEtag >= maxEtag && stats.Duration >= _configuration.MapTimeoutAfterEtagReached.AsTimeSpan)
            {
                stats.RecordMapCompletedReason($"Reached maximum etag that was seen when batch started ({maxEtag:#,#;;0}) and map duration ({stats.Duration}) exceeded configured limit ({_configuration.MapTimeoutAfterEtagReached.AsTimeSpan})");
                return false;
            }

            if (_index.ShouldReleaseTransactionBecauseFlushIsWaiting(stats))
                return false;

            if (_index.CanContinueBatch(stats, queryContext, indexingContext, indexWriter, count) == false)
                return false;

            return true;
        }

        protected abstract IEnumerable<IndexItem> GetItemsEnumerator(QueryOperationContext queryContext, string collection, long lastEtag, long pageSize);
    }
}
