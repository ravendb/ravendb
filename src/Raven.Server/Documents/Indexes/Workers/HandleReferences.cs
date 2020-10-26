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
using Voron;

namespace Raven.Server.Documents.Indexes.Workers
{
    public class HandleDocumentReferences : HandleReferences
    {
        protected override ReferenceType Type => ReferenceType.Documents;

        public HandleDocumentReferences(Index index, Dictionary<string, HashSet<CollectionName>> referencedCollections, DocumentsStorage documentsStorage, IndexStorage indexStorage, IndexingConfiguration configuration)
            : this(index, referencedCollections, documentsStorage, indexStorage, indexStorage.ReferencesForDocuments, configuration)
        {
        }

        protected HandleDocumentReferences(Index index, Dictionary<string, HashSet<CollectionName>> referencedCollections, DocumentsStorage documentsStorage, IndexStorage indexStorage, IndexStorage.ReferencesBase referencesStorage, IndexingConfiguration configuration)
            : base(index, referencedCollections, documentsStorage, indexStorage, referencesStorage, configuration)
        {
        }

        protected override unsafe IndexItem GetItem(DocumentsOperationContext databaseContext, Slice key)
        {
            using (DocumentIdWorker.GetLower(databaseContext.Allocator, key.Content.Ptr, key.Size, out var loweredKey))
            {
                // when there is conflict, we need to apply same behavior as if the document would not exist
                var doc = _documentsStorage.Get(databaseContext, loweredKey, throwOnConflict: false);
                if (doc == null)
                    return default;

                return new DocumentIndexItem(doc.Id, doc.LowerId, doc.Etag, doc.LastModified, doc.Data.Size, doc);
            }
        }

        public override void HandleDelete(Tombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            var tx = indexContext.Transaction.InnerTransaction;

            using (Slice.External(tx.Allocator, tombstone.LowerId, out Slice tombstoneKeySlice))
                _referencesStorage.RemoveReferences(tombstoneKeySlice, collection, null, indexContext.Transaction);
        }
    }

    public abstract class HandleReferences : HandleReferencesBase
    {
        private readonly Dictionary<string, HashSet<CollectionName>> _referencedCollections;

        protected HandleReferences(Index index, Dictionary<string, HashSet<CollectionName>> referencedCollections, DocumentsStorage documentsStorage, IndexStorage indexStorage, IndexStorage.ReferencesBase referencesStorage, IndexingConfiguration configuration)
            : base(index, documentsStorage, indexStorage, referencesStorage, configuration)
        {
            _referencedCollections = referencedCollections;
        }

        protected override bool TryGetReferencedCollectionsFor(string collection, out HashSet<CollectionName> referencedCollections)
        {
            return _referencedCollections.TryGetValue(collection, out referencedCollections);
        }
    }

    public abstract class HandleReferencesBase : IIndexingWork
    {
        private readonly Logger _logger;

        private readonly Index _index;

        protected readonly DocumentsStorage _documentsStorage;
        private readonly IndexingConfiguration _configuration;
        protected readonly IndexStorage _indexStorage;
        protected readonly IndexStorage.ReferencesBase _referencesStorage;
        protected readonly Reference _reference = new Reference();

        protected HandleReferencesBase(Index index, DocumentsStorage documentsStorage, IndexStorage indexStorage, IndexStorage.ReferencesBase referencesStorage, IndexingConfiguration configuration)
        {
            _index = index;
            _documentsStorage = documentsStorage;
            _configuration = configuration;
            _indexStorage = indexStorage;
            _referencesStorage = referencesStorage;
            _logger = LoggingSource.Instance
                .GetLogger<HandleReferences>(_indexStorage.DocumentDatabase.Name);
        }

        public string Name => "References";

        protected virtual bool ItemsAndReferencesAreUsingSameEtagPool => true;

        public bool Execute(QueryOperationContext queryContext, TransactionOperationContext indexContext,
            Lazy<IndexWriteOperation> writeOperation, IndexingStatsScope stats, CancellationToken token)
        {
            const long pageSize = long.MaxValue;
            var maxTimeForDocumentTransactionToRemainOpen = Debugger.IsAttached == false
                            ? _configuration.MaxTimeForDocumentTransactionToRemainOpen.AsTimeSpan
                            : TimeSpan.FromMinutes(15);

            var moreWorkFound = HandleItems(ActionType.Tombstone, queryContext, indexContext, writeOperation, stats, pageSize, maxTimeForDocumentTransactionToRemainOpen, token);
            moreWorkFound |= HandleItems(ActionType.Document, queryContext, indexContext, writeOperation, stats, pageSize, maxTimeForDocumentTransactionToRemainOpen, token);

            return moreWorkFound;
        }

        protected abstract bool TryGetReferencedCollectionsFor(string collection, out HashSet<CollectionName> referencedCollections);

        private unsafe bool HandleItems(ActionType actionType, QueryOperationContext queryContext, TransactionOperationContext indexContext, Lazy<IndexWriteOperation> writeOperation, IndexingStatsScope stats, long pageSize, TimeSpan maxTimeForDocumentTransactionToRemainOpen, CancellationToken token)
        {
            var moreWorkFound = false;
            Dictionary<string, long> lastIndexedEtagsByCollection = null;

            var totalProcessedCount = 0;
            foreach (var collection in _index.Collections)
            {
                if (TryGetReferencedCollectionsFor(collection, out var referencedCollections) == false)
                    continue;

                if (lastIndexedEtagsByCollection == null)
                    lastIndexedEtagsByCollection = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

                if (lastIndexedEtagsByCollection.TryGetValue(collection, out long lastIndexedEtag) == false)
                    lastIndexedEtagsByCollection[collection] = lastIndexedEtag = _indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection);

                if (lastIndexedEtag == 0) // we haven't indexed yet, so we are skipping references for now
                    continue;

                var reference = _index.LastProcessedReferences.For(Type, actionType, collection);
                foreach (var referencedCollection in referencedCollections)
                {
                    var inMemoryStats = _index.GetReferencesStats(referencedCollection.Name);

                    using (var collectionStats = stats.For("Collection_" + referencedCollection.Name))
                    {
                        long lastReferenceEtag;

                        switch (actionType)
                        {
                            case ActionType.Document:
                                lastReferenceEtag =
                                    _referencesStorage.ReadLastProcessedReferenceEtag(indexContext.Transaction.InnerTransaction, collection, referencedCollection);
                                break;
                            case ActionType.Tombstone:
                                lastReferenceEtag = _referencesStorage.ReadLastProcessedReferenceTombstoneEtag(indexContext.Transaction.InnerTransaction, collection,
                                    referencedCollection);
                                break;
                            default:
                                throw new NotSupportedException();
                        }

                        var lastEtag = lastReferenceEtag;
                        var resultsCount = 0;

                        var sw = new Stopwatch();
                        IndexWriteOperation indexWriter = null;

                        var keepRunning = true;
                        var earlyExit = false;
                        var lastCollectionEtag = -1L;
                        while (keepRunning)
                        {
                            var hasChanges = false;
                            var indexed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                            using (queryContext.OpenReadTransaction())
                            {
                                sw.Restart();

                                IEnumerable<Reference> references;
                                switch (actionType)
                                {
                                    case ActionType.Document:
                                        if (lastCollectionEtag == -1)
                                            lastCollectionEtag = _index.GetLastItemEtagInCollection(queryContext, collection);

                                        references = GetItemReferences(queryContext, referencedCollection, lastEtag, pageSize);
                                        break;
                                    case ActionType.Tombstone:
                                        if (lastCollectionEtag == -1)
                                            lastCollectionEtag = _index.GetLastTombstoneEtagInCollection(queryContext, collection);

                                        references = GetTombstoneReferences(queryContext, referencedCollection, lastEtag, pageSize);
                                        break;
                                    default:
                                        throw new NotSupportedException();
                                }

                                foreach (var referencedDocument in references)
                                {
                                    hasChanges = true;

                                    var items = GetItemsFromCollectionThatReference(queryContext, indexContext, collection, referencedDocument, lastIndexedEtag, indexed, reference);

                                    using (var itemsEnumerator = _index.GetMapEnumerator(items, collection, indexContext, collectionStats, _index.Type))
                                    {
                                        while (itemsEnumerator.MoveNext(queryContext.Documents, out IEnumerable mapResults, out var etag))
                                        {
                                            token.ThrowIfCancellationRequested();

                                            var current = itemsEnumerator.Current;
                                            indexWriter ??= writeOperation.Value;

                                            if (CanContinueReferenceBatch() == false)
                                            {
                                                reference.Set(referencedDocument, current.Id);
                                                earlyExit = true;
                                                break;
                                            }

                                            totalProcessedCount++;
                                            collectionStats.RecordMapReferenceAttempt();
                                            stats.RecordDocumentSize(current.Size);

                                            try
                                            {
                                                var numberOfResults = _index.HandleMap(current, mapResults, indexWriter, indexContext, collectionStats);

                                                resultsCount += numberOfResults;
                                                collectionStats.RecordMapReferenceSuccess();
                                                _index.MapsPerSec?.MarkSingleThreaded(numberOfResults);
                                            }
                                            catch (Exception e) when (e.IsIndexError())
                                            {
                                                itemsEnumerator.OnError();
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

                                    if (earlyExit)
                                        break;

                                    lastEtag = referencedDocument.Etag;
                                    inMemoryStats.UpdateLastEtag(lastEtag, actionType == ActionType.Tombstone);

                                    if (CanContinueReferenceBatch() == false)
                                        break;
                                }

                                if (hasChanges == false)
                                    break;
                            }

                            bool CanContinueReferenceBatch()
                            {
                                if (_index.CanContinueBatch(stats, queryContext, indexContext, indexWriter, lastEtag, lastCollectionEtag, totalProcessedCount) == false)
                                {
                                    keepRunning = false;
                                    return false;
                                }

                                if (totalProcessedCount >= pageSize)
                                {
                                    keepRunning = false;
                                    return false;
                                }

                                if (MapItems.MaybeRenewTransaction(queryContext, sw, _configuration, ref maxTimeForDocumentTransactionToRemainOpen))
                                    return false;

                                return true;
                            }
                        }

                        if (lastReferenceEtag == lastEtag)
                        {
                            // the last referenced etag hasn't changed
                            if (keepRunning == false && earlyExit)
                                return true;

                            continue;
                        }

                        moreWorkFound = true;

                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Executed handle references for '{_index.Name}' index and '{referencedCollection.Name}' collection. " +
                                         $"Got {resultsCount:#,#;;0} map results in {collectionStats.Duration.TotalMilliseconds:#,#;;0} ms.");

                        switch (actionType)
                        {
                            case ActionType.Document:
                                _referencesStorage.WriteLastReferenceEtag(indexContext.Transaction, collection, referencedCollection, lastEtag);
                                break;
                            case ActionType.Tombstone:
                                _referencesStorage.WriteLastReferenceTombstoneEtag(indexContext.Transaction, collection, referencedCollection, lastEtag);
                                break;
                            default:
                                throw new NotSupportedException();
                        }

                        reference.Clear(earlyExit);
                    }
                }
            }

            if (moreWorkFound == false)
                _index.LastProcessedReferences.ClearForType(Type, actionType);

            return moreWorkFound;
        }

        private IEnumerable<IndexItem> GetItemsFromCollectionThatReference(QueryOperationContext queryContext, TransactionOperationContext indexContext,
            string collection, Reference referencedDocument, long lastIndexedEtag, HashSet<string> indexed, ReferencesState.Reference reference)
        {
            var lastProcessedItemId = reference.GetLastProcessedItemId(referencedDocument);
            foreach (var key in _referencesStorage.GetItemKeysFromCollectionThatReference(collection, referencedDocument.Key, indexContext.Transaction, lastProcessedItemId))
            {
                var item = GetItem(queryContext.Documents, key);
                if (item == null)
                    continue;

                if (indexed.Add(item.Id) == false)
                {
                    item.Dispose();
                    continue;
                }

                if (item.Etag > lastIndexedEtag)
                {
                    item.Dispose();
                    continue;
                }

                if (ItemsAndReferencesAreUsingSameEtagPool && item.Etag > referencedDocument.Etag)
                {
                    //If the map worker already mapped this "doc" version it must be with this version of "referencedDocument" and if the map worker didn't mapped the "doc" so it will process it later
                    item.Dispose();
                    continue;
                }

                yield return item;
            }
        }

        protected virtual IEnumerable<Reference> GetItemReferences(QueryOperationContext queryContext, CollectionName referencedCollection, long lastEtag, long pageSize)
        {
            return _documentsStorage
                .GetDocumentsFrom(queryContext.Documents, referencedCollection.Name, lastEtag + 1, 0, pageSize, DocumentFields.Id)
                .Select(document =>
                {
                    _reference.Key = document.Id;
                    _reference.Etag = document.Etag;

                    return _reference;
                });
        }

        protected virtual IEnumerable<Reference> GetTombstoneReferences(QueryOperationContext queryContext, CollectionName referencedCollection, long lastEtag, long pageSize)
        {
            return _documentsStorage
                .GetTombstonesFrom(queryContext.Documents, referencedCollection.Name, lastEtag + 1, 0, pageSize)
                .Select(tombstone =>
                {
                    _reference.Key = tombstone.LowerId;
                    _reference.Etag = tombstone.Etag;

                    return _reference;
                });
        }

        protected abstract ReferenceType Type { get; }

        protected abstract IndexItem GetItem(DocumentsOperationContext databaseContext, Slice key);

        public abstract void HandleDelete(Tombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats);

        public class Reference
        {
            public LazyStringValue Key;

            public long Etag;
        }

        public enum ActionType
        {
            Document,
            Tombstone
        }

        public enum ReferenceType
        {
            Documents,
            Counters,
            TimeSeries,
            CompareExchangeCounters,
            CompareExchangeTimeSeries
        }
    }
}
