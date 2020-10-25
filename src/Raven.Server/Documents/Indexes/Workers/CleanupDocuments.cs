using System;
using System.Diagnostics;
using System.Threading;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.Documents.Indexes.Workers
{
    public class CleanupDocuments : IIndexingWork
    {
        private readonly Logger _logger;

        private readonly Index _index;
        private readonly IndexingConfiguration _configuration;
        private readonly DocumentsStorage _documentsStorage;
        private readonly IndexStorage _indexStorage;
        private readonly MapReduceIndexingContext _mapReduceContext;

        public CleanupDocuments(Index index, DocumentsStorage documentsStorage, IndexStorage indexStorage,
            IndexingConfiguration configuration, MapReduceIndexingContext mapReduceContext)
        {
            _index = index;
            _configuration = configuration;
            _mapReduceContext = mapReduceContext;
            _documentsStorage = documentsStorage;
            _indexStorage = indexStorage;
            _logger = LoggingSource.Instance
                .GetLogger<CleanupDocuments>(indexStorage.DocumentDatabase.Name);
        }

        public string Name => "Cleanup";

        public virtual bool Execute(QueryOperationContext queryContext, TransactionOperationContext indexContext,
            Lazy<IndexWriteOperation> writeOperation, IndexingStatsScope stats, CancellationToken token)
        {
            const long pageSize = long.MaxValue;
            var maxTimeForDocumentTransactionToRemainOpen = Debugger.IsAttached == false
                ? _configuration.MaxTimeForDocumentTransactionToRemainOpen.AsTimeSpan
                : TimeSpan.FromMinutes(15);

            var moreWorkFound = false;

            foreach (var collection in _index.Collections)
            {
                using (var collectionStats = stats.For("Collection_" + collection))
                {
                    var lastMappedEtag = _indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection);
                    var lastTombstoneEtag = _indexStorage.ReadLastProcessedTombstoneEtag(indexContext.Transaction, collection);

                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Executing cleanup for '{_index} ({_index.Name})'. Collection: {collection}. LastMappedEtag: {lastMappedEtag:#,#;;0}. LastTombstoneEtag: {lastTombstoneEtag:#,#;;0}.");

                    var inMemoryStats = _index.GetStats(collection);
                    var lastEtag = lastTombstoneEtag;
                    var count = 0;

                    var sw = new Stopwatch();
                    IndexWriteOperation indexWriter = null;
                    var keepRunning = true;
                    var lastCollectionEtag = -1L;
                    while (keepRunning)
                    {
                        var batchCount = 0L;

                        using (queryContext.OpenReadTransaction())
                        {
                            sw.Restart();

                            if (lastCollectionEtag == -1)
                                lastCollectionEtag = _index.GetLastTombstoneEtagInCollection(queryContext, collection);

                            var tombstones = collection == Constants.Documents.Collections.AllDocumentsCollection
                                ? _documentsStorage.GetTombstonesFrom(queryContext.Documents, lastEtag + 1, 0, pageSize)
                                : _documentsStorage.GetTombstonesFrom(queryContext.Documents, collection, lastEtag + 1, 0, pageSize);

                            foreach (var tombstone in tombstones)
                            {
                                token.ThrowIfCancellationRequested();

                                if (indexWriter == null)
                                    indexWriter = writeOperation.Value;

                                count++;
                                batchCount++;
                                lastEtag = tombstone.Etag;
                                inMemoryStats.UpdateLastEtag(lastEtag, isTombstone: true);

                                if (_logger.IsInfoEnabled && count % 2048 == 0)
                                    _logger.Info($"Executing cleanup for '{_index.Name}'. Processed count: {count:#,#;;0} etag: {lastEtag}.");

                                if (tombstone.Type != Tombstone.TombstoneType.Document)
                                    continue; // this can happen when we have '@all_docs'

                                _index.HandleDelete(tombstone, collection, indexWriter, indexContext, collectionStats);

                                if (CanContinueBatch(queryContext, indexContext, collectionStats, indexWriter, lastEtag, lastCollectionEtag, batchCount) == false)
                                {
                                    keepRunning = false;
                                    break;
                                }

                                if (MapItems.MaybeRenewTransaction(queryContext, sw, _configuration, ref maxTimeForDocumentTransactionToRemainOpen))
                                    break;
                            }

                            if (batchCount == 0 || batchCount >= pageSize)
                                break;
                        }
                    }

                    if (count == 0)
                        continue;

                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Executing cleanup for '{_index} ({_index.Name})'. Processed {count} tombstones in '{collection}' collection in {collectionStats.Duration.TotalMilliseconds:#,#;;0} ms.");

                    if (_index.Type.IsMap())
                    {
                        _indexStorage.WriteLastTombstoneEtag(indexContext.Transaction, collection, lastEtag);
                    }
                    else
                    {
                        _mapReduceContext.ProcessedTombstoneEtags[collection] = lastEtag;
                    }

                    moreWorkFound = true;
                }
            }

            return moreWorkFound;
        }

        public bool CanContinueBatch(
            QueryOperationContext queryContext,
            TransactionOperationContext indexingContext,
            IndexingStatsScope stats,
            IndexWriteOperation indexWriteOperation,
            long currentEtag,
            long maxEtag,
            long count)
        {
            if (_index.ShouldRunCanContinueBatch(count) == false)
                return true;

            if (stats.Duration >= _configuration.MapTimeout.AsTimeSpan)
                return false;

            if (currentEtag >= maxEtag && stats.Duration >= _configuration.MapTimeoutAfterEtagReached.AsTimeSpan)
                return false;

            if (_index.CanContinueBatch(stats, queryContext, indexingContext, indexWriteOperation, count) == false)
                return false;

            return true;
        }
    }
}
