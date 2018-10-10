using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;

namespace Raven.Server.Documents.Indexes.Workers
{
    public class HandleReferences : IIndexingWork
    {
        private readonly Logger _logger;

        private readonly Index _index;
        private readonly Dictionary<string, HashSet<CollectionName>> _referencedCollections;
        private readonly IndexingConfiguration _configuration;
        private readonly DocumentsStorage _documentsStorage;
        private readonly IndexStorage _indexStorage;

        public HandleReferences(Index index, Dictionary<string, HashSet<CollectionName>> referencedCollections, DocumentsStorage documentsStorage, IndexStorage indexStorage, IndexingConfiguration configuration)
        {
            _index = index;
            _referencedCollections = referencedCollections;
            _configuration = configuration;
            _documentsStorage = documentsStorage;
            _indexStorage = indexStorage;
            _logger = LoggingSource.Instance
                .GetLogger<HandleReferences>(_indexStorage.DocumentDatabase.Name);
        }

        public string Name => "References";

        public bool Execute(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext,
            Lazy<IndexWriteOperation> writeOperation, IndexingStatsScope stats, CancellationToken token)
        {
            const int pageSize = int.MaxValue;
            var maxTimeForDocumentTransactionToRemainOpen = Debugger.IsAttached == false
                            ? _configuration.MaxTimeForDocumentTransactionToRemainOpen.AsTimeSpan
                            : TimeSpan.FromMinutes(15);

            var moreWorkFound = HandleDocuments(ActionType.Tombstone, databaseContext, indexContext, writeOperation, stats, pageSize, maxTimeForDocumentTransactionToRemainOpen, token);
            moreWorkFound |= HandleDocuments(ActionType.Document, databaseContext, indexContext, writeOperation, stats, pageSize, maxTimeForDocumentTransactionToRemainOpen, token);

            return moreWorkFound;
        }

        public bool CanContinueBatch(DocumentsOperationContext documentsContext, TransactionOperationContext indexingContext, IndexingStatsScope stats, long currentEtag, long maxEtag, int count)
        {
            if (stats.Duration >= _configuration.MapTimeout.AsTimeSpan)
                return false;

            if (currentEtag >= maxEtag && stats.Duration >= _configuration.MapTimeoutAfterEtagReached.AsTimeSpan)
                return false;

            if (_index.CanContinueBatch(stats, documentsContext, indexingContext, count) == false)
                return false;

            return true;
        }

        private unsafe bool HandleDocuments(ActionType actionType, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, Lazy<IndexWriteOperation> writeOperation, IndexingStatsScope stats, int pageSize, TimeSpan maxTimeForDocumentTransactionToRemainOpen, CancellationToken token)
        {
            var moreWorkFound = false;
            Dictionary<string, long> lastIndexedEtagsByCollection = null;

            foreach (var collection in _index.Collections)
            {
                if (_referencedCollections.TryGetValue(collection, out HashSet<CollectionName> referencedCollections) == false)
                    continue;

                if (lastIndexedEtagsByCollection == null)
                    lastIndexedEtagsByCollection = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

                if (lastIndexedEtagsByCollection.TryGetValue(collection, out long lastIndexedEtag) == false)
                    lastIndexedEtagsByCollection[collection] = lastIndexedEtag = _indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection);

                if (lastIndexedEtag == 0) // we haven't indexed yet, so we are skipping references for now
                    continue;

                foreach (var referencedCollection in referencedCollections)
                {
                    using (var collectionStats = stats.For("Collection_" + referencedCollection.Name))
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Executing handle references for '{_index.Name}'. Collection: {referencedCollection.Name}. Type: {actionType}.");

                        long lastReferenceEtag;
                        switch (actionType)
                        {
                            case ActionType.Document:
                                lastReferenceEtag = _indexStorage.ReadLastProcessedReferenceEtag(indexContext.Transaction, collection, referencedCollection);
                                break;
                            case ActionType.Tombstone:
                                lastReferenceEtag = _indexStorage.ReadLastProcessedReferenceTombstoneEtag(indexContext.Transaction, collection, referencedCollection);
                                break;
                            default:
                                throw new NotSupportedException();
                        }
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Executing handle references for '{_index.Name}'. LastReferenceEtag: {lastReferenceEtag}.");

                        var lastEtag = lastReferenceEtag;
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

                                IEnumerable<Reference> references;
                                switch (actionType)
                                {
                                    case ActionType.Document:
                                        if (lastCollectionEtag == -1)
                                            lastCollectionEtag = _index.GetLastDocumentEtagInCollection(databaseContext, collection);

                                        references = _documentsStorage
                                            .GetDocumentsFrom(databaseContext, referencedCollection.Name, lastEtag + 1, 0, pageSize)
                                            .Select(document => new Reference{Etag = document.Etag, Key = document.Id });
                                        break;
                                    case ActionType.Tombstone:
                                        if (lastCollectionEtag == -1)
                                            lastCollectionEtag = _index.GetLastTombstoneEtagInCollection(databaseContext, collection);

                                        references = _documentsStorage
                                            .GetTombstonesFrom(databaseContext, referencedCollection.Name, lastEtag + 1, 0, pageSize)
                                            .Select(tombstone => new Reference { Etag = tombstone.Etag, Key = tombstone.LowerId });
                                        break;
                                    default:
                                        throw new NotSupportedException();
                                }

                                var list = references.ToList();
                                foreach (var referencedDocument in list)
                                {
                                    if (_logger.IsInfoEnabled)
                                        _logger.Info($"Executing handle references for '{_index.Name}'. Processing reference: {referencedDocument.Key}.");

                                    lastEtag = referencedDocument.Etag;
                                    count++;
                                    batchCount++;

                                    var documents = new List<Document>();
                                    foreach (var key in _indexStorage
                                        .GetDocumentKeysFromCollectionThatReference(collection, referencedDocument.Key, indexContext.Transaction))
                                    {
                                        using (DocumentIdWorker.GetLower(databaseContext.Allocator, key.Content.Ptr, key.Size, out var loweredKey))
                                        {
                                            // when there is conflict, we need to apply same behavior as if the document would not exist
                                            var doc = _documentsStorage.Get(databaseContext, loweredKey, throwOnConflict: false);

                                            if (doc != null && doc.Etag <= lastIndexedEtag)
                                                documents.Add(doc);
                                        }
                                    }

                                    using (var docsEnumerator = _index.GetMapEnumerator(documents, collection, indexContext, collectionStats, _index.Type))
                                    {
                                        while (docsEnumerator.MoveNext(out IEnumerable mapResults))
                                        {
                                            token.ThrowIfCancellationRequested();

                                            var current = docsEnumerator.Current;

                                            if (indexWriter == null)
                                                indexWriter = writeOperation.Value;

                                            if (_logger.IsInfoEnabled)
                                                _logger.Info($"Executing handle references for '{_index.Name}'. Processing document: {current.Id}.");

                                            try
                                            {
                                                _index.HandleMap(current.LowerId, current.Id, mapResults, indexWriter, indexContext, collectionStats);
                                            }
                                            catch (Exception e)
                                            {
                                                if (_logger.IsInfoEnabled)
                                                    _logger.Info($"Failed to execute mapping function on '{current.Id}' for '{_index.Name}'.", e);
                                            }
                                        }
                                    }

                                    if (CanContinueBatch(databaseContext, indexContext, collectionStats, lastEtag, lastCollectionEtag, batchCount) == false)
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
                            _logger.Info($"Executing handle references for '{_index} ({_index.Name})'. Processed {count} references in '{referencedCollection.Name}' collection in {collectionStats.Duration.TotalMilliseconds:#,#;;0} ms.");

                        switch (actionType)
                        {
                            case ActionType.Document:
                                _indexStorage.WriteLastReferenceEtag(indexContext.Transaction, collection, referencedCollection, lastEtag);
                                break;
                            case ActionType.Tombstone:
                                _indexStorage.WriteLastReferenceTombstoneEtag(indexContext.Transaction, collection, referencedCollection, lastEtag);
                                break;
                            default:
                                throw new NotSupportedException();
                        }

                        moreWorkFound = true;
                    }
                }
            }
            return moreWorkFound;
        }

        public unsafe void HandleDelete(Tombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            var tx = indexContext.Transaction.InnerTransaction;
            var loweredKey = tombstone.LowerId;
            using (Slice.External(tx.Allocator, loweredKey.Buffer, loweredKey.Size, out Slice tombstoneKeySlice))
                _indexStorage.RemoveReferences(tombstoneKeySlice, collection, null, indexContext.Transaction);
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
