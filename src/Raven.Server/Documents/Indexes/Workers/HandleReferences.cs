using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
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

        private readonly Reference _reference = new Reference();

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

        public bool CanContinueBatch(DocumentsOperationContext documentsContext, TransactionOperationContext indexingContext, 
            IndexingStatsScope stats, IndexWriteOperation indexWriteOperation, long currentEtag, long maxEtag, int count)
        {
            if (stats.Duration >= _configuration.MapTimeout.AsTimeSpan)
                return false;

            if (_configuration.MapBatchSize.HasValue && count >= _configuration.MapBatchSize.Value)
                return false;

            if (currentEtag >= maxEtag && stats.Duration >= _configuration.MapTimeoutAfterEtagReached.AsTimeSpan)
                return false;

            if (_index.ShouldReleaseTransactionBecauseFlushIsWaiting(stats))
                return false;

            if (_index.CanContinueBatch(stats, documentsContext, indexingContext, indexWriteOperation, count) == false)
                return false;

            return true;
        }

        private bool HandleDocuments(ActionType actionType, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, Lazy<IndexWriteOperation> writeOperation, IndexingStatsScope stats, int pageSize, TimeSpan maxTimeForDocumentTransactionToRemainOpen, CancellationToken token)
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

                var totalProcessedCount = 0;

                foreach (var referencedCollection in referencedCollections)
                {
                    var inMemoryStats = _index.GetReferencesStats(referencedCollection.Name);

                    using (var collectionStats = stats.For("Collection_" + referencedCollection.Name))
                    {
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
                            _logger.Info($"Executing handle references for '{_index.Name}'. LastReferenceEtag: {lastReferenceEtag}. Collection: {referencedCollection.Name}. Type: {actionType}.");

                        var lastEtag = lastReferenceEtag;
                        var resultsCount = 0;

                        var sw = new Stopwatch();
                        IndexWriteOperation indexWriter = null;

                        var keepRunning = true;
                        var lastCollectionEtag = -1L;
                        while (keepRunning)
                        {
                            var hasChanges = false;

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
                                            .GetDocumentsFrom(databaseContext, referencedCollection.Name, lastEtag + 1, 0, pageSize, 
                                                DocumentFields.Id | DocumentFields.Etag)
                                            .Select(document =>
                                            {
                                                _reference.Key = document.Id;
                                                _reference.Etag = document.Etag;

                                                return _reference;
                                            });
                                        break;
                                    case ActionType.Tombstone:
                                        if (lastCollectionEtag == -1)
                                            lastCollectionEtag = _index.GetLastTombstoneEtagInCollection(databaseContext, collection);

                                        references = _documentsStorage
                                            .GetTombstonesFrom(databaseContext, referencedCollection.Name, lastEtag + 1, 0, pageSize)
                                            .Select(tombstone =>
                                            {
                                                _reference.Key = tombstone.LowerId;
                                                _reference.Etag = tombstone.Etag;

                                                return _reference;
                                            });
                                        break;
                                    default:
                                        throw new NotSupportedException();
                                }

                                var isTombstone = actionType == ActionType.Tombstone;

                                foreach (var referencedDocument in references)
                                {
                                    if (_logger.IsInfoEnabled)
                                        _logger.Info($"Executing handle references for '{_index.Name}'. Processing reference: {referencedDocument.Key}.");

                                    lastEtag = referencedDocument.Etag;
                                    hasChanges = true;
                                    inMemoryStats.UpdateLastEtag(lastEtag, isTombstone);

                                    var documents = GetDocumentFromCollectionThatReference(databaseContext, indexContext, collection, referencedDocument, lastIndexedEtag);

                                    using (var docsEnumerator = _index.GetMapEnumerator(documents, collection, indexContext, collectionStats, _index.Type))
                                    {
                                        while (docsEnumerator.MoveNext(out IEnumerable mapResults))
                                        {
                                            token.ThrowIfCancellationRequested();

                                            totalProcessedCount++;
                                            collectionStats.RecordMapReferenceAttempt();

                                            var current = docsEnumerator.Current;

                                            if (indexWriter == null)
                                                indexWriter = writeOperation.Value;

                                            if (_logger.IsInfoEnabled)
                                                _logger.Info($"Executing handle references for '{_index.Name}'. Processing document: {current.Id}.");

                                            try
                                            {
                                                var numberOfResults = _index.HandleMap(current.LowerId, current.Id, mapResults, indexWriter, indexContext, collectionStats);
                                                
                                                resultsCount += numberOfResults;
                                                collectionStats.RecordMapReferenceSuccess();
                                                _index.MapsPerSec.MarkSingleThreaded(numberOfResults);
                                            }
                                            catch (Exception e) when (e.IsIndexError())
                                            {
                                                docsEnumerator.OnError();
                                                _index.ErrorIndexIfCriticalException(e);

                                                collectionStats.RecordMapReferenceError();
                                                if (_logger.IsInfoEnabled)
                                                    _logger.Info($"Failed to execute mapping function on '{current.Id}' for '{_index.Name}'.", e);

                                                collectionStats.AddMapReferenceError(current.Id, 
                                                    $"Failed to execute mapping function on {current.Id}. Exception: {e}");
                                            }

                                            _index.UpdateThreadAllocations(indexContext, indexWriter, stats, updateReduceStats: false);
                                        }
                                    }

                                    if (CanContinueBatch(databaseContext, indexContext, collectionStats, indexWriter, lastEtag, lastCollectionEtag, totalProcessedCount) == false)
                                    {
                                        keepRunning = false;
                                        break;
                                    }

                                    if (totalProcessedCount >= pageSize)
                                    {
                                        keepRunning = false;
                                        break;
                                    }

                                    if (MapDocuments.MaybeRenewTransaction(databaseContext, sw, _configuration, ref maxTimeForDocumentTransactionToRemainOpen))
                                        break;
                                }

                                if (hasChanges == false)
                                    break;
                            }
                        }

                        if (lastReferenceEtag == lastEtag)
                        {
                            // the last referenced etag hasn't changed
                            continue;
                        }

                        moreWorkFound = true;

                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Executed handle references for '{_index.Name}' index and '{referencedCollection.Name}' collection. " +
                                         $"Got {resultsCount:#,#;;0} map results in {collectionStats.Duration.TotalMilliseconds:#,#;;0} ms.");

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
                    }
                }
            }

            return moreWorkFound;
        }

        private IEnumerable<Document> GetDocumentFromCollectionThatReference(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, string collection, Reference referencedDocument, long lastIndexedEtag)
        {
            foreach (var key in _indexStorage.GetDocumentKeysFromCollectionThatReference(collection, referencedDocument.Key, indexContext.Transaction))
            {
                using (GetLower(out Slice loweredKey))
                {
                    // when there is conflict, we need to apply same behavior as if the document would not exist
                    var doc = _documentsStorage.Get(databaseContext, loweredKey, throwOnConflict: false);

                    if (doc == null)
                        continue;

                    if (doc.Etag > lastIndexedEtag)
                    {
                        doc.Dispose();
                        continue;
                    }

                    if (doc.Etag > referencedDocument.Etag)
                    {
                        //IF the map worker already mapped this "doc" version it must be with this version of "referencedDocument" and if the map worker didn't mapped the "doc" so it will process it later 
                        doc.Dispose();
                        continue;
                    }

                    yield return doc;
                }

                unsafe ByteStringContext<ByteStringMemoryCache>.InternalScope GetLower(out Slice loweredKey)
                {
                    return DocumentIdWorker.GetLower(databaseContext.Allocator, key.Content.Ptr, key.Size, out loweredKey);
                }
            }
        }

        public unsafe void HandleDelete(Tombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            var tx = indexContext.Transaction.InnerTransaction;
            var loweredKey = tombstone.LowerId;
            using (Slice.External(tx.Allocator, loweredKey, out Slice tombstoneKeySlice))
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
