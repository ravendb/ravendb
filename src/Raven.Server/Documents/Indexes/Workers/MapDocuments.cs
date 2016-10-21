using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Data.Indexes;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.Documents.Indexes.Workers
{
    public class MapDocuments : IIndexingWork
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
                            ? _configuration.MaxTimeForDocumentTransactionToRemainOpenInSec.AsTimeSpan
                            : TimeSpan.FromMinutes(15);

            var moreWorkFound = false;
            foreach (var collection in _index.Collections)
            {
                using (var collectionStats = stats.For("Collection_" + collection))
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Executing map for '{_index.Name} ({_index.IndexId})'. Collection: {collection}.");

                    var lastMappedEtag = _indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection);

                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Executing map for '{_index.Name} ({_index.IndexId})'. LastMappedEtag: {lastMappedEtag}.");

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
                                    IEnumerable mapResults;
                                    if (docsEnumerator.MoveNext(out mapResults) == false)
                                    {
                                        collectionStats.RecordMapCompletedReason("No more documents to index");
                                        keepRunning = false;
                                        break;
                                    }

                                    token.ThrowIfCancellationRequested();

                                    if (indexWriter == null)
                                        indexWriter = writeOperation.Value;

                                    var current = docsEnumerator.Current;

                                    if (_logger.IsInfoEnabled)
                                        _logger.Info(
                                            $"Executing map for '{_index.Name} ({_index.IndexId})'. Processing document: {current.Key}.");

                                    collectionStats.RecordMapAttempt();

                                    count++;
                                    lastEtag = current.Etag;

                                    try
                                    {
                                        var numberOfResults = _index.HandleMap(current.LoweredKey, mapResults,
                                            indexWriter, indexContext, collectionStats);
                                        _index.MapsPerSec.Mark(numberOfResults);
                                        resultsCount += numberOfResults;
                                        collectionStats.RecordMapSuccess();
                                    }
                                    catch (Exception e)
                                    {
                                        _index.HandleError(e);

                                        collectionStats.RecordMapError();
                                        if (_logger.IsInfoEnabled)
                                            _logger.Info(
                                                $"Failed to execute mapping function on '{current.Key}' for '{_index.Name} ({_index.IndexId})'.",
                                                e);

                                        collectionStats.AddMapError(current.Key,
                                            $"Failed to execute mapping function on {current.Key}. Exception: {e}");
                                    }

                                    if (CanContinueBatch(collectionStats, lastEtag, lastCollectionEtag) == false)
                                    {
                                        keepRunning = false;
                                        break;
                                    }

                                    if (count >= pageSize)
                                    {
                                        keepRunning = false;
                                        break;
                                    }

                                    if (sw.Elapsed > maxTimeForDocumentTransactionToRemainOpen)
                                        break;
                                }
                            }
                        }
                    }

                    if (count == 0)
                        continue;

                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Executing map for '{_index.Name} ({_index.IndexId})'. Processed {count:#,#;;0} documents and {resultsCount:#,#;;0} map results in '{collection}' collection in {collectionStats.Duration.TotalMilliseconds:#,#;;0} ms.");

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

        public bool CanContinueBatch(IndexingStatsScope stats, long currentEtag, long maxEtag)
        {
            if (currentEtag >= maxEtag)
                return false;

            if (_index.CanContinueBatch(stats) == false)
                return false;

            return true;
        }

        private IEnumerable<Document> GetDocumentsEnumerator(DocumentsOperationContext databaseContext, string collection, long lastEtag, int pageSize)
        {
            if (collection == Constants.Indexing.AllDocumentsCollection)
                return _documentsStorage.GetDocumentsFrom(databaseContext, lastEtag + 1, 0, pageSize);
            return _documentsStorage.GetDocumentsFrom(databaseContext, collection, lastEtag + 1, 0, pageSize);
        }
    }
}