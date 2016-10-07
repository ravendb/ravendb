using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Data.Indexes;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.LowMemoryNotification;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Documents.Indexes.Workers
{
    public class MapDocuments : IIndexingWork
    {
        protected Logger _logger;
        private readonly Index _index;
        private readonly MapReduceIndexingContext _mapReduceContext;
        private readonly DocumentsStorage _documentsStorage;
        private readonly IndexStorage _indexStorage;

        public MapDocuments(Index index, DocumentsStorage documentsStorage, IndexStorage indexStorage, MapReduceIndexingContext mapReduceContext)
        {
            _index = index;
            _mapReduceContext = mapReduceContext;
            _documentsStorage = documentsStorage;
            _indexStorage = indexStorage;
            _logger = LoggingSource.Instance
                .GetLogger<MapDocuments>(indexStorage.DocumentDatabase.Name);
        }

        public string Name => "Map";

        public bool Execute(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext,
            Lazy<IndexWriteOperation> writeOperation, IndexingStatsScope stats, CancellationToken token)
        {
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

                    var sw = Stopwatch.StartNew();
                    IndexWriteOperation indexWriter = null;
                    var maxTimeForReadTxToRemainOpen = TimeSpan.FromSeconds(15);
                    var keepRunning = true;
                    while (keepRunning)
                    {
                        using (databaseContext.OpenReadTransaction())
                        {
                            var documents = GetDocumentsEnumerator(databaseContext, collection, lastEtag);

                            using (var docsEnumerator = _index.GetMapEnumerator(documents, collection, indexContext,collectionStats))
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
                                        collectionStats.RecordMapError();
                                        if (_logger.IsInfoEnabled)
                                            _logger.Info(
                                                $"Failed to execute mapping function on '{current.Key}' for '{_index.Name} ({_index.IndexId})'.",
                                                e);

                                        collectionStats.AddMapError(current.Key,
                                            $"Failed to execute mapping function on {current.Key}. Exception: {e}");
                                    }

                                    if (_index.CanContinueBatch(collectionStats) == false)
                                    {
                                        keepRunning = false;
                                        break;
                                    }
                                    if (sw.Elapsed > maxTimeForReadTxToRemainOpen)
                                        break;
                                }
                            }
                        }
                    }

                    if (count == 0)
                        continue;

                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Executing map for '{_index.Name} ({_index.IndexId})'. Processed {count:#,#;;0} documents and {resultsCount:#,#;;0} map results in '{collection}' collection in {sw.ElapsedMilliseconds:#,#;;0} ms.");

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

        private IEnumerable<Document> GetDocumentsEnumerator(DocumentsOperationContext databaseContext, string collection, long lastEtag)
        {
            var maxValue = int.MaxValue;
            if (collection == Constants.Indexing.AllDocumentsCollection)
                return _documentsStorage.GetDocumentsAfter(databaseContext, lastEtag + 1, 0, maxValue);
            return _documentsStorage.GetDocumentsAfter(databaseContext, collection, lastEtag + 1, 0, maxValue);
        }
    }
}