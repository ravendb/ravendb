using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Logging;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Indexes.Workers
{
    public class HandleReferences : IIndexingWork
    {
        protected readonly ILog Log = LogManager.GetLogger(typeof(CleanupDeletedDocuments));

        private readonly Index _index;
        private readonly StaticMapIndex _staticIndex;
        private readonly IndexingConfiguration _configuration;
        private readonly DocumentsStorage _documentsStorage;
        private readonly IndexStorage _indexStorage;

        public HandleReferences(StaticMapIndex index, DocumentsStorage documentsStorage, IndexStorage indexStorage, IndexingConfiguration configuration)
        {
            _index = index;
            _staticIndex = index;
            _configuration = configuration;
            _documentsStorage = documentsStorage;
            _indexStorage = indexStorage;
        }

        public string Name => "References";

        public bool Execute(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext,
            Lazy<IndexWriteOperation> writeOperation, IndexingStatsScope stats, CancellationToken token)
        {
            var pageSize = _configuration.MaxNumberOfDocumentsToFetchForMap;
            var timeoutProcessing = Debugger.IsAttached == false ? _configuration.DocumentProcessingTimeout.AsTimeSpan : TimeSpan.FromMinutes(15);

            Dictionary<string, long> lastIndexedEtagsByCollection = null;

            var moreWorkFound = false;

            foreach (var referencedCollection in _index.ReferencedCollections)
            {
                var lastReferenceEtag = _indexStorage.ReadLastProcessedReferenceEtag(indexContext.Transaction, referencedCollection);
                var lastEtag = lastReferenceEtag;
                var count = 0;

                var sw = Stopwatch.StartNew();
                IndexWriteOperation indexWriter = null;

                using (databaseContext.OpenReadTransaction())
                {
                    foreach (var referencedDocument in _documentsStorage.GetDocumentsAfter(databaseContext, referencedCollection, lastReferenceEtag + 1, 0, pageSize))
                    {
                        lastEtag = referencedDocument.Etag;
                        count++;

                        foreach (var collection in _index.Collections)
                        {
                            var lastSeenEtag = _indexStorage.ReadLastSeenEtagForReference(collection, referencedDocument.Key, indexContext.Transaction);
                            if (referencedDocument.Etag == lastSeenEtag)
                                continue;

                            if (lastIndexedEtagsByCollection == null)
                                lastIndexedEtagsByCollection = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

                            long lastIndexedEtag;
                            if (lastIndexedEtagsByCollection.TryGetValue(collection, out lastIndexedEtag) == false)
                                lastIndexedEtagsByCollection[collection] = lastIndexedEtag = _indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection);

                            var documents = _indexStorage
                                .GetDocumentKeysFromCollectionThatReference(collection, referencedDocument.Key, indexContext.Transaction)
                                .Select(key => _documentsStorage.Get(databaseContext, key))
                                .Where(doc => doc != null)
                                .Where(doc => doc.Etag <= lastIndexedEtag);

                            var stateful = new StatefulEnumerator<Document>(documents);
                            foreach (var document in _index.EnumerateMap(stateful, collection, indexContext))
                            {
                                token.ThrowIfCancellationRequested();

                                var current = stateful.Current;

                                if (indexWriter == null)
                                    indexWriter = writeOperation.Value;

                                try
                                {
                                    _index.HandleMap(current.Key, document, indexWriter, indexContext, stats);
                                }
                                catch (Exception e)
                                {
                                    // TODO
                                }

                                if (sw.Elapsed > timeoutProcessing)
                                    break;
                            }
                        }
                    }
                }

                if (count == 0)
                    continue;

                //if (Log.IsDebugEnabled)
                //    Log.Debug($"Executing cleanup for '{_index} ({_index.Name})'. Processed {count} tombstones in '{collection}' collection in {sw.ElapsedMilliseconds:#,#;;0} ms.");

                _indexStorage.WriteLastReferenceEtag(indexContext.Transaction, referencedCollection, lastEtag);

                moreWorkFound = true;
            }

            return moreWorkFound;
        }
    }
}