using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;
using Voron;

namespace Raven.Server.Documents.Indexes.Workers
{
    public sealed class MapDocuments : IIndexingWork
    {
        private readonly Logger _logger;
        private readonly Index _index;
        private readonly MapReduceIndexingContext _mapReduceContext;
        private readonly IndexingConfiguration _configuration;
        private readonly DocumentsStorage _documentsStorage;
        private readonly IndexStorage _indexStorage;

        public MapDocuments(Index index, DocumentsStorage documentsStorage, IndexStorage indexStorage, MapReduceIndexingContext mapReduceContext, IndexingConfiguration configuration)
        {
            _index = index;
            _mapReduceContext = mapReduceContext;
            _configuration = configuration;
            _documentsStorage = documentsStorage;
            _indexStorage = indexStorage;
            _logger = LoggingSource.Instance
                .GetLogger<MapDocuments>(indexStorage.DocumentDatabase.Name);
        }

        public string Name => "Map";

        public bool Execute(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext,
            Lazy<IndexWriteOperation> writeOperation, IndexingStatsScope stats, CancellationToken token)
        {
            var maxTimeForDocumentTransactionToRemainOpen = Debugger.IsAttached == false
                            ? _configuration.MaxTimeForDocumentTransactionToRemainOpen.AsTimeSpan
                            : TimeSpan.FromMinutes(15);

            var moreWorkFound = false;
            foreach (var collection in _index.Collections)
            {
                using (var collectionStats = stats.For("Collection_" + collection))
                {
                    var lastMappedEtag = _indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection);

                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Executing map for '{_index.Name}'. Collection: {collection} LastMappedEtag: {lastMappedEtag:#,#;;0}.");

                    var inMemoryStats = _index.GetStats(collection);
                    var lastEtag = lastMappedEtag;
                    var count = 0;
                    var resultsCount = 0;
                    var pageSize = int.MaxValue;

                    var sw = new Stopwatch();
                    IndexWriteOperation indexWriter = null;
                    var keepRunning = true;
                    var lastCollectionEtag = -1L;
                    while (keepRunning)
                    {
                        using (databaseContext.OpenReadTransaction())
                        {
                            sw.Restart();

                            if (lastCollectionEtag == -1)
                                lastCollectionEtag = _index.GetLastDocumentEtagInCollection(databaseContext, collection);

                            var documents = GetDocumentsEnumerator(databaseContext, collection, lastEtag, pageSize);

                            using (var docsEnumerator = _index.GetMapEnumerator(documents, collection, indexContext, collectionStats))
                            {
                                while (true)
                                {
                                    if (docsEnumerator.MoveNext(out IEnumerable mapResults) == false)
                                    {
                                        collectionStats.RecordMapCompletedReason("No more documents to index");
                                        keepRunning = false;
                                        break;
                                    }

                                    token.ThrowIfCancellationRequested();

                                    if (indexWriter == null)
                                        indexWriter = writeOperation.Value;

                                    var current = docsEnumerator.Current;

                                    count++;
                                    collectionStats.RecordMapAttempt();
                                    stats.RecordDocumentSize(current.Data.Size);
                                    if (_logger.IsInfoEnabled && count % 8192 == 0)
                                        _logger.Info($"Executing map for '{_index.Name}'. Processed count: {count:#,#;;0} etag: {lastEtag}.");

                                    lastEtag = current.Etag;
                                    inMemoryStats.UpdateLastEtag(lastEtag, isTombsone: false);

                                    try
                                    {
                                        var numberOfResults = _index.HandleMap(current.LowerId, mapResults,
                                            indexWriter, indexContext, collectionStats);
                                        _index.MapsPerSec.Mark(numberOfResults);
                                        resultsCount += numberOfResults;
                                        collectionStats.RecordMapSuccess();
                                    }
                                    catch (Exception e)
                                    {
                                        docsEnumerator.OnError();
                                        _index.ErrorIndexIfCriticalException(e);

                                        collectionStats.RecordMapError();
                                        if (_logger.IsInfoEnabled)
                                            _logger.Info($"Failed to execute mapping function on '{current.Id}' for '{_index.Name}'.", e);

                                        collectionStats.AddMapError(current.Id, $"Failed to execute mapping function on {current.Id}. " +
                                                                                $"Exception: {e}");
                                    }

                                    if (CanContinueBatch(databaseContext, indexContext, collectionStats, lastEtag, lastCollectionEtag, count) == false)
                                    {
                                        keepRunning = false;
                                        break;
                                    }

                                    if (count >= pageSize)
                                    {
                                        keepRunning = false;
                                        break;
                                    }

                                    if (MaybeRenewTransaction(databaseContext, sw, _configuration, ref maxTimeForDocumentTransactionToRemainOpen))
                                        break;
                                }

                                if (_logger.IsInfoEnabled)
                                    _logger.Info($"Done executing map for '{_index.Name}'. Proccessed count: {count:#,#;;0} LastEtag {lastEtag}.");
                            }
                        }
                    }

                    if (count == 0)
                        continue;

                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Executing map for '{_index.Name}'. Processed {count:#,#;;0} documents and {resultsCount:#,#;;0} map results in '{collection}' collection in {collectionStats.Duration.TotalMilliseconds:#,#;;0} ms.");

                    if (_index.Type.IsMap())
                    {
                        _indexStorage.WriteLastIndexedEtag(indexContext.Transaction, collection, lastEtag);
                    }
                    else
                    {
                        _mapReduceContext.ProcessedDocEtags[collection] = lastEtag;
                    }

                    moreWorkFound = true;
                }
            }

            return moreWorkFound;
        }

        public static bool MaybeRenewTransaction(
            DocumentsOperationContext databaseContext, Stopwatch sw,
            IndexingConfiguration configuration,
            ref TimeSpan maxTimeForDocumentTransactionToRemainOpen)
        {
            if (sw.Elapsed > maxTimeForDocumentTransactionToRemainOpen)
            {
                if (databaseContext.ShouldRenewTransactionsToAllowFlushing())
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

        private DateTime _lastCheckedFlushLock;

        private bool ShouldReleaseTransactionBecauseFlushIsWaiting(IndexingStatsScope stats)
        {
            if (GlobalFlushingBehavior.GlobalFlusher.Value.HasLowNumberOfFlushingResources == false)
                return false;

            var now = DateTime.UtcNow;
            if ((now - _lastCheckedFlushLock).TotalSeconds < 1)
                return false;

            _lastCheckedFlushLock = now;

            var gotLock = _index._indexStorage.Environment().FlushInProgressLock.TryEnterReadLock(0);
            try
            {
                if (gotLock == false)
                {
                    stats.RecordMapCompletedReason("Environment flush was waiting for us and global flusher was about to use all free flushing resources");
                    return true;
                }
            }
            finally
            {
                if (gotLock)
                    _index._indexStorage.Environment().FlushInProgressLock.ExitReadLock();
            }
            return false;
        }

        public bool CanContinueBatch(DocumentsOperationContext documentsContext, TransactionOperationContext indexingContext, IndexingStatsScope stats, long currentEtag, long maxEtag, int count)
        {
            if (stats.Duration >= _configuration.MapTimeout.AsTimeSpan)
            {
                stats.RecordMapCompletedReason($"Exceeded maximum configured map duration ({_configuration.MapTimeout.AsTimeSpan}). Was {stats.Duration}");
                return false;
            }

            if (currentEtag >= maxEtag && stats.Duration >= _configuration.MapTimeoutAfterEtagReached.AsTimeSpan)
            {
                stats.RecordMapCompletedReason($"Reached maximum etag that was seen when batch started ({maxEtag:#,#;;0}) and map duration ({stats.Duration}) exceeded configured limit ({_configuration.MapTimeoutAfterEtagReached.AsTimeSpan})");
                return false;
            }

            if (ShouldReleaseTransactionBecauseFlushIsWaiting(stats))
                return false;

            if (_index.CanContinueBatch(stats, documentsContext, indexingContext, count) == false)
                return false;

            return true;
        }

        private IEnumerable<Document> GetDocumentsEnumerator(DocumentsOperationContext databaseContext, string collection, long lastEtag, int pageSize)
        {
            if (collection == Constants.Documents.Collections.AllDocumentsCollection)
                return _documentsStorage.GetDocumentsFrom(databaseContext, lastEtag + 1, 0, pageSize);
            return _documentsStorage.GetDocumentsFrom(databaseContext, collection, lastEtag + 1, 0, pageSize);
        }
    }
}
