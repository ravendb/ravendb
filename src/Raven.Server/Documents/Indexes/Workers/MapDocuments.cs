using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Abstractions.Logging;
using Raven.Client.Data.Indexes;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;

namespace Raven.Server.Documents.Indexes.Workers
{
    public class MapDocuments : IIndexingWork
    {
        protected Logger _logger;

        private readonly Index _index;
        private readonly IndexingConfiguration _configuration;
        private readonly MapReduceIndexingContext _mapReduceContext;
        private readonly DocumentsStorage _documentsStorage;
        private readonly IndexStorage _indexStorage;

        public MapDocuments(Index index, DocumentsStorage documentsStorage, IndexStorage indexStorage, 
                            IndexingConfiguration configuration, MapReduceIndexingContext mapReduceContext)
        {
            _index = index;
            _configuration = configuration;
            _mapReduceContext = mapReduceContext;
            _documentsStorage = documentsStorage;
            _indexStorage = indexStorage;
            _logger = LoggerSetup.Instance
                .GetLogger<MapDocuments>(indexStorage.DocumentDatabase.Name);
        }

        public string Name => "Map";

        public bool Execute(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext,
            Lazy<IndexWriteOperation> writeOperation, IndexingStatsScope stats, CancellationToken token)
        {
            var pageSize = _configuration.MaxNumberOfDocumentsToFetchForMap;
            var timeoutProcessing = Debugger.IsAttached == false ? _configuration.DocumentProcessingTimeout.AsTimeSpan : TimeSpan.FromMinutes(15);

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

                    var sw = Stopwatch.StartNew();
                    IndexWriteOperation indexWriter = null;

                    using (databaseContext.OpenReadTransaction())
                    {
                        var documents = _documentsStorage.GetDocumentsAfter(databaseContext, collection, lastEtag + 1, 0, pageSize);

                        using (var docsEnumerator = _index.GetMapEnumerator(documents, collection, indexContext))
                        {
                            IEnumerable mapResults;

                            while (docsEnumerator.MoveNext(out mapResults))
                            {
                                //TODO: take into account time here, if we are on slow i/o system, we don't want to wait for 128K docs before
                                //TODO: we flush the index
                                token.ThrowIfCancellationRequested();

                                if (indexWriter == null)
                                    indexWriter = writeOperation.Value;

                                var current = docsEnumerator.Current;

                                if (_logger.IsInfoEnabled)
                                    _logger.Info($"Executing map for '{_index.Name} ({_index.IndexId})'. Processing document: {current.Key}.");

                                collectionStats.RecordMapAttempt();

                                count++;
                                lastEtag = current.Etag;

                                try
                                {
                                    _index.HandleMap(current.Key, mapResults, indexWriter, indexContext, collectionStats);

                                    collectionStats.RecordMapSuccess();
                                }
                                catch (Exception e)
                                {
                                    collectionStats.RecordMapError();
                                    if (_logger.IsInfoEnabled)
                                        _logger.Info($"Failed to execute mapping function on '{current.Key}' for '{_index.Name} ({_index.IndexId})'.",e);

                                    collectionStats.AddMapError(current.Key,
                                        $"Failed to execute mapping function on {current.Key}. Message: {e.Message}");
                                }

                                if (sw.Elapsed > timeoutProcessing)
                                    break;
                            }
                        }
                    }

                    if (count == 0)
                        continue;

                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Executing map for '{_index.Name} ({_index.IndexId})'. Processed {count} documents in '{collection}' collection in {sw.ElapsedMilliseconds:#,#;;0} ms.");

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
    }
}