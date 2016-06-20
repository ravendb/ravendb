using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Logging;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Workers
{
    public class HandleReferences : IIndexingWork
    {
        protected readonly ILog Log = LogManager.GetLogger(typeof(CleanupDeletedDocuments));

        private readonly Index _index;
        private readonly IndexingConfiguration _configuration;
        private readonly DocumentsStorage _documentsStorage;
        private readonly IndexStorage _indexStorage;

        private readonly Reference _reference = new Reference();

        public HandleReferences(Index index, DocumentsStorage documentsStorage, IndexStorage indexStorage, IndexingConfiguration configuration)
        {
            _index = index;
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

            var moreWorkFound = HandleDocuments(ActionType.Tombstone, databaseContext, indexContext, writeOperation, stats, pageSize, timeoutProcessing, token);
            moreWorkFound |= HandleDocuments(ActionType.Document, databaseContext, indexContext, writeOperation, stats, pageSize, timeoutProcessing, token);

            return moreWorkFound;
        }

        private bool HandleDocuments(ActionType actionType, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, Lazy<IndexWriteOperation> writeOperation, IndexingStatsScope stats, int pageSize, TimeSpan timeoutProcessing, CancellationToken token)
        {
            var moreWorkFound = false;
            Dictionary<string, long> lastIndexedEtagsByCollection = null;

            foreach (var referencedCollection in _index.ReferencedCollections)
            {
                long lastReferenceEtag;

                switch (actionType)
                {
                    case ActionType.Document:
                        lastReferenceEtag = _indexStorage.ReadLastProcessedReferenceEtag(indexContext.Transaction, referencedCollection);
                        break;
                    case ActionType.Tombstone:
                        lastReferenceEtag = _indexStorage.ReadLastProcessedReferenceTombstoneEtag(indexContext.Transaction, referencedCollection);
                        break;
                    default:
                        throw new NotSupportedException();
                }

                var lastEtag = lastReferenceEtag;
                var count = 0;

                var sw = Stopwatch.StartNew();
                IndexWriteOperation indexWriter = null;

                using (databaseContext.OpenReadTransaction())
                {
                    IEnumerable<Reference> references;
                    switch (actionType)
                    {
                        case ActionType.Document:
                            references = _documentsStorage
                                .GetDocumentsAfter(databaseContext, referencedCollection, lastReferenceEtag + 1, 0, pageSize)
                                .Select(document =>
                                {
                                    _reference.Key = document.Key;
                                    _reference.Etag = document.Etag;

                                    return _reference;
                                });
                            break;
                        case ActionType.Tombstone:
                            references = _documentsStorage
                                .GetTombstonesAfter(databaseContext, referencedCollection, lastReferenceEtag + 1, 0, pageSize)
                                .Select(tombstone =>
                                {
                                    _reference.Key = tombstone.Key;
                                    _reference.Etag = tombstone.Etag;

                                    return _reference;
                                });
                            break;
                        default:
                            throw new NotSupportedException();
                    }

                    foreach (var referencedDocument in references)
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

                if (Log.IsDebugEnabled)
                    Log.Debug($"Executing handle references for '{_index} ({_index.Name})'. Processed {count} references in '{referencedCollection}' collection in {sw.ElapsedMilliseconds:#,#;;0} ms.");

                switch (actionType)
                {
                    case ActionType.Document:
                        _indexStorage.WriteLastReferenceEtag(indexContext.Transaction, referencedCollection, lastEtag);
                        break;
                    case ActionType.Tombstone:
                        _indexStorage.WriteLastReferenceTombstoneEtag(indexContext.Transaction, referencedCollection, lastEtag);
                        break;
                    default:
                        throw new NotSupportedException();
                }

                moreWorkFound = true;
            }

            return moreWorkFound;
        }

        private enum ActionType
        {
            Document,
            Tombstone
        }

        private class Reference
        {
            public LazyStringValue Key;

            public long Etag;
        }
    }
}