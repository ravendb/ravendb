using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Logging;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;

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

            var lastIndexedEtagsByCollection = _index.Collections
                .ToDictionary(collection => collection, collection => _indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection));

            var moreWorkFound = false;

            foreach (var referencedCollection in _indexStorage.GetReferencedCollections(indexContext.Transaction))
            {
                var lastReferenceEtag = _indexStorage.ReadLastProcessedReferenceEtag(indexContext.Transaction, referencedCollection);
                var lastEtag = lastReferenceEtag;
                var count = 0;

                var sw = Stopwatch.StartNew();

                using (databaseContext.OpenReadTransaction())
                {
                    foreach (var referencedDocument in _documentsStorage.GetDocumentsAfter(databaseContext, referencedCollection, lastReferenceEtag + 1, 0, pageSize))
                    {
                        lastEtag = referencedDocument.Etag;
                        count++;

                        // probably we will need to split this per collection
                        // because indexingFunc needs enumerator that only returns
                        // documents from one collection
                        foreach (var key in _indexStorage.GetDocumentKeysThatReference(referencedDocument.Key, indexContext.Transaction))
                        {
                            token.ThrowIfCancellationRequested();

                            var document = _documentsStorage.Get(databaseContext, key);
                            if (document == null)
                                continue;

                            bool isSystemDocument;
                            var collection = Document.GetCollectionName(key, document.Data, out isSystemDocument);

                            var lastIndexedEtag = lastIndexedEtagsByCollection[collection];
                            if (document.Etag > lastIndexedEtag)
                                continue; // it will be indexed

                            var lastSeenEtag = _indexStorage.ReadLastSeenEtagForReference(collection, referencedDocument.Key, indexContext.Transaction);
                            if (referencedDocument.Etag == lastSeenEtag)
                                continue;

                            Debug.Assert(referencedDocument.Etag >= lastIndexedEtag);

                            // index here

                            if (sw.Elapsed > timeoutProcessing)
                                break;
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