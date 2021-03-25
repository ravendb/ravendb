using System;
using System.Diagnostics;
using System.Threading;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.MapReduce.Static;
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

        public virtual bool Execute(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext,
            Lazy<IndexWriteOperation> writeOperation, IndexingStatsScope stats, CancellationToken token)
        {
            const int pageSize = int.MaxValue;
            var maxTimeForDocumentTransactionToRemainOpen = Debugger.IsAttached == false
                ? _configuration.MaxTimeForDocumentTransactionToRemainOpen.AsTimeSpan
                : TimeSpan.FromMinutes(15);

            var moreWorkFound = false;

            foreach (var collection in _index.Collections)
            {
                var shouldExtraLogs = _logger.IsOperationsEnabled && collection.Equals("executiontasks", StringComparison.OrdinalIgnoreCase);

                using (var collectionStats = stats.For("Collection_" + collection))
                {
                    var lastMappedEtag = _indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection);
                    var lastTombstoneEtag = _indexStorage.ReadLastProcessedTombstoneEtag(indexContext.Transaction, collection);

                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info(
                            $"Executing cleanup for '{_index} ({_index.Name})'. Collection: {collection}. LastMappedEtag: {lastMappedEtag:#,#;;0}. LastTombstoneEtag: {lastTombstoneEtag:#,#;;0}.");
                    }
                    else if (shouldExtraLogs)
                    {
                        _logger.Operations(
                            $"Executing cleanup for '{_index} ({_index.Name})'. Collection: {collection}. LastMappedEtag: {lastMappedEtag:#,#;;0}. LastTombstoneEtag: {lastTombstoneEtag:#,#;;0}.");
                    }

                    var inMemoryStats = _index.GetStats(collection);
                    var lastEtag = lastTombstoneEtag;
                    var count = 0;

                    var sw = new Stopwatch();
                    IndexWriteOperation indexWriter = null;
                    var keepRunning = true;
                    var lastCollectionEtag = -1L;
                    while (keepRunning)
                    {
                        var batchCount = 0;

                        using (databaseContext.OpenReadTransaction())
                        {
                            sw.Restart();

                            if (lastCollectionEtag == -1)
                                lastCollectionEtag = _index.GetLastTombstoneEtagInCollection(databaseContext, collection);

                            var tombstones = collection == Constants.Documents.Collections.AllDocumentsCollection
                                ? _documentsStorage.GetTombstonesFrom(databaseContext, lastEtag + 1, 0, pageSize)
                                : _documentsStorage.GetTombstonesFrom(databaseContext, collection, lastEtag + 1, 0, pageSize);

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
                                {
                                    _logger.Info($"Executing cleanup for '{_index.Name}'. Processed count: {count:#,#;;0} etag: {lastEtag}.");
                                }
                                else if (shouldExtraLogs && count % 2048 == 0)
                                {
                                    _logger.Operations($"Executing cleanup for '{_index.Name}'. Processed count: {count:#,#;;0} etag: {lastEtag}.");
                                }

                                if (tombstone.Type != Tombstone.TombstoneType.Document)
                                    continue; // this can happen when we have '@all_docs'

                                if (shouldExtraLogs)
                                    _logger.Operations($"{nameof(_index.HandleDelete)} - Id:{tombstone.LowerId}, DeletedEtag: {tombstone.DeletedEtag}, Etag: {tombstone.Etag}, ChangeVector: {tombstone.ChangeVector}");
                                _index.HandleDelete(tombstone, collection, indexWriter, indexContext, collectionStats);

                                if (CanContinueBatch(databaseContext, indexContext, collectionStats, indexWriter, lastEtag, lastCollectionEtag, batchCount) == false)
                                {
                                    keepRunning = false;
                                    break;
                                }

                                if (MapDocuments.MaybeRenewTransaction(databaseContext, sw, _configuration, ref maxTimeForDocumentTransactionToRemainOpen))
                                    break;
                            }

                            if (batchCount == 0 || batchCount >= pageSize)
                                break;
                        }
                    }

                    if (count == 0)
                        continue;

                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info(
                            $"Executing cleanup for '{_index} ({_index.Name})'. Processed {count} tombstones in '{collection}' collection in {collectionStats.Duration.TotalMilliseconds:#,#;;0} ms.");
                    }
                    else if (shouldExtraLogs)
                    {
                        _logger.Operations(
                            $"Executing cleanup for '{_index} ({_index.Name})'. Processed {count} tombstones in '{collection}' collection in {collectionStats.Duration.TotalMilliseconds:#,#;;0} ms.");
                    }

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
            DocumentsOperationContext documentsContext,
            TransactionOperationContext indexingContext,
            IndexingStatsScope stats,
            IndexWriteOperation indexWriteOperation,
            long currentEtag,
            long maxEtag,
            int count)
        {
            if (stats.Duration >= _configuration.MapTimeout.AsTimeSpan)
                return false;

            if (currentEtag >= maxEtag && stats.Duration >= _configuration.MapTimeoutAfterEtagReached.AsTimeSpan)
                return false;

            if (_index.CanContinueBatch(stats, documentsContext, indexingContext, indexWriteOperation, count) == false)
                return false;

            return true;
        }
    }
}
