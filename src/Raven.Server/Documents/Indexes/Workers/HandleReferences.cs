﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;

namespace Raven.Server.Documents.Indexes.Workers
{
    public class HandleDocumentReferences : HandleReferences
    {
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

                return new DocumentIndexItem(doc.Id, doc.LowerId, doc.Etag, doc.LastModified, doc.Data.Size, doc, doc.Flags);
            }
        }

        public override void HandleDelete(Tombstone tombstone, string collection, Lazy<IndexWriteOperationBase> writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
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

    public abstract partial class HandleReferencesBase : IIndexingWork
    {
        private readonly RavenLogger _logger;

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
            _logger = RavenLogManager.Instance.GetLoggerForIndex(GetType(), index);
        }

        public string Name => "References";

        protected virtual bool ItemsAndReferencesAreUsingSameEtagPool => true;

        public (bool MoreWorkFound, Index.CanContinueBatchResult BatchContinuationResult) Execute(QueryOperationContext queryContext, TransactionOperationContext indexContext,
            Lazy<IndexWriteOperationBase> writeOperation, IndexingStatsScope stats, CancellationToken token)
        {
            const long pageSize = long.MaxValue;
            var maxTimeForDocumentTransactionToRemainOpen = Debugger.IsAttached == false
                            ? _configuration.MaxTimeForDocumentTransactionToRemainOpen.AsTimeSpan
                            : TimeSpan.FromMinutes(15);

            var tombstonesHandlingResult = HandleItems(ActionType.Tombstone, queryContext, indexContext, writeOperation, stats, pageSize, maxTimeForDocumentTransactionToRemainOpen, token);
            var documentsHandlingResult = HandleItems(ActionType.Document, queryContext, indexContext, writeOperation, stats, pageSize, maxTimeForDocumentTransactionToRemainOpen, token);

            var batchContinuationResult = Index.CanContinueBatchResult.True;

            if (tombstonesHandlingResult.BatchContinuationResult == Index.CanContinueBatchResult.False || documentsHandlingResult.BatchContinuationResult == Index.CanContinueBatchResult.False)
                batchContinuationResult = Index.CanContinueBatchResult.False;

            return (tombstonesHandlingResult.MoreWorkFound | documentsHandlingResult.MoreWorkFound, batchContinuationResult);
        }

        protected abstract bool TryGetReferencedCollectionsFor(string collection, out HashSet<CollectionName> referencedCollections);

        private (bool MoreWorkFound, Index.CanContinueBatchResult BatchContinuationResult) HandleItems(ActionType actionType, QueryOperationContext queryContext, TransactionOperationContext indexContext, Lazy<IndexWriteOperationBase> writeOperation, IndexingStatsScope stats, long pageSize, TimeSpan maxTimeForDocumentTransactionToRemainOpen, CancellationToken token)
        {
            var moreWorkFound = false;
            var batchContinuationResult = Index.CanContinueBatchResult.None;

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

                // When opening the read transaction for a single map covering all of its referenced collections,
                // we can efficiently skip indexing documents that have already been indexed.
                var readTransaction = queryContext.OpenReadTransaction();
                var indexed = new HashSet<Slice>(SliceComparer.Instance);

                try
                {
                    var referenceState = indexContext.GetReferenceStateFor(actionType, collection);
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

                            var keepRunning = true;
                            var earlyExit = false;
                            var lastCollectionEtag = -1L;

                            while (keepRunning)
                            {
                                UpdateReferences(indexContext, collection);

                                RenewTransactionIfNeeded(queryContext, batchContinuationResult, indexed, ref readTransaction);

                                var hasChanges = false;
                                earlyExit = false;

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

                                foreach (var referencedItem in references)
                                {
                                    hasChanges = true;

                                    using (referencedItem)
                                    {
                                        if (ItemsAndReferencesAreUsingSameEtagPool && _index._indexStorage.LowerThanLastDatabaseEtagOnIndexCreation(referencedItem.Etag))
                                        {
                                            // the referenced item will be indexed in the map step
                                            lastEtag = referencedItem.Etag;
                                            inMemoryStats.UpdateLastEtag(lastEtag, actionType == ActionType.Tombstone);
                                            totalProcessedCount++;

                                            token.ThrowIfCancellationRequested();

                                            if (CanContinueReferenceBatch() == false)
                                                break;

                                            continue;
                                        }

                                        var items = GetItemsFromCollectionThatReference(queryContext, indexContext, collection, referencedItem, lastIndexedEtag, indexed, referenceState);

                                        var numberOfReferencedItemLoad = 0;

                                        using (var itemsEnumerator = _index.GetMapEnumerator(items, collection, indexContext, collectionStats, _index.Type))
                                        {
                                            long lastIndexedParentEtag = 0;
                                            while (itemsEnumerator.MoveNext(queryContext.Documents, out IEnumerable mapResults, out var etag))
                                            {
                                                token.ThrowIfCancellationRequested();

                                                var current = itemsEnumerator.Current;

                                                if (CanContinueReferenceBatch() == false)
                                                {
                                                    // updating the last reference state in order to continue from the place we left off
                                                    referenceState = new ReferenceState(referencedItem.Key, referencedItem.Etag, current.LowerSourceDocumentId ?? current.Id, lastIndexedParentEtag);

                                                    if (batchContinuationResult != Index.CanContinueBatchResult.RenewTransaction)
                                                    {
                                                        // we save where we last stopped in order to continue running in a NEW indexing batch
                                                        indexContext.SetReferencesStateFor(actionType, collection, referenceState);
                                                    }

                                                    earlyExit = true;
                                                    break;
                                                }

                                                lastIndexedParentEtag = current.Etag;
                                                totalProcessedCount++;
                                                collectionStats.RecordMapReferenceAttempt();
                                                stats.RecordDocumentSize(current.Size);

                                                numberOfReferencedItemLoad++;

                                                try
                                                {
                                                    var numberOfResults = _index.HandleMap(current, mapResults, writeOperation, indexContext, collectionStats);

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

                                                _index.UpdateThreadAllocations(indexContext, writeOperation, stats, IndexingWorkType.References);
                                            }
                                        }

                                        if (numberOfReferencedItemLoad > 0) 
                                            _index.CheckReferenceLoadsPerformanceHintLimit(referencedItem, numberOfReferencedItemLoad);

                                        if (earlyExit)
                                            break;

                                        lastEtag = referencedItem.Etag;
                                        inMemoryStats.UpdateLastEtag(lastEtag, actionType == ActionType.Tombstone);

                                        if (CanContinueReferenceBatch() == false)
                                            break;
                                    }
                                }

                                if (hasChanges == false)
                                    break;

                                _index._forTestingPurposes?.BeforeClosingDocumentsReadTransactionForHandleReferences?.Invoke();

                                bool CanContinueReferenceBatch()
                                {
                                    var parameters = new CanContinueBatchParameters(stats, IndexingWorkType.References, queryContext, indexContext, writeOperation,
                                        lastEtag, lastCollectionEtag, totalProcessedCount, sw);

                                    batchContinuationResult = _index.CanContinueBatch(in parameters, ref maxTimeForDocumentTransactionToRemainOpen);
                                    if (batchContinuationResult != Index.CanContinueBatchResult.True)
                                    {
                                        keepRunning = batchContinuationResult == Index.CanContinueBatchResult.RenewTransaction;
                                        return false;
                                    }

                                    if (totalProcessedCount >= pageSize)
                                    {
                                        keepRunning = false;
                                        return false;
                                    }

                                    return true;
                                }
                            }

                            if (lastReferenceEtag == lastEtag)
                            {
                                // the last referenced etag hasn't changed
                                if (keepRunning == false && earlyExit)
                                    return (true, batchContinuationResult);

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

                            if (earlyExit == false)
                            {
                                indexContext.ClearReferencesStateFor(actionType, collection);
                        }
                    }
                }
                }
                finally
                {
                    foreach (var slice in indexed)
                    {
                        slice.Release(queryContext.Documents.Allocator);
                    }

                    readTransaction?.Dispose();
                }
            }

            if (moreWorkFound == false)
                indexContext.ClearReferencesState(actionType);

            return (moreWorkFound, batchContinuationResult);
        }

        private void UpdateReferences(TransactionOperationContext indexContext, string collection)
        {
            // References were found during handling references
            // (HandleReferences is the first worker that is running so those references were found here).
            // When we have a LoadDocument we save the referenced document in-memory (ReferencesByCollection) and at the end of the batch we store it in the storage
            // BUT we process the references from the storage only
            // In order to avoid skipping handling the found references, we must save them in the storage

            if (CurrentIndexingScope.Current.ReferencesByCollection != null &&
                CurrentIndexingScope.Current.ReferencesByCollection.TryGetValue(collection, out var values))
            {
                _indexStorage.ReferencesForDocuments.WriteReferencesForSingleCollection(collection, values, indexContext.Transaction);
                values.Clear();
            }

            if (CurrentIndexingScope.Current.ReferencesByCollectionForCompareExchange != null &&
                CurrentIndexingScope.Current.ReferencesByCollectionForCompareExchange.TryGetValue(collection, out values))
            {
                _indexStorage.ReferencesForCompareExchange.WriteReferencesForSingleCollection(collection, values, indexContext.Transaction);
                values.Clear();
            }
        }

        private static void RenewTransactionIfNeeded(QueryOperationContext queryContext, Index.CanContinueBatchResult batchContinuationResult, HashSet<Slice> indexed, ref IDisposable readTransaction)
        {
            if (batchContinuationResult != Index.CanContinueBatchResult.RenewTransaction)
                return;

            var toDispose = readTransaction;
            readTransaction = null; // prevent double dispose in case of an error
            toDispose.Dispose();

            foreach (var slice in indexed)
            {
                slice.Release(queryContext.Documents.Allocator);
            }

            // the documents that have already been indexed become irrelevant once the new transaction is opened.
            indexed.Clear();

            readTransaction = queryContext.OpenReadTransaction();
        }

        private IEnumerable<IndexItem> GetItemsFromCollectionThatReference(QueryOperationContext queryContext, TransactionOperationContext indexContext,
            string collection, Reference referencedItem, long lastIndexedEtag, HashSet<Slice> indexed, ReferenceState referenceState)
        {
            var lastProcessedItemId = referenceState?.GetLastProcessedItemId(referencedItem);
            foreach (var key in _referencesStorage.GetItemKeysFromCollectionThatReference(collection, referencedItem.Key, indexContext.Transaction, lastProcessedItemId))
            {
                // we check if we already indexed the document BEFORE we fetch it from the disk.
                if (indexed.Contains(key))
                    continue;

                indexed.Add(key.Clone(queryContext.Documents.Allocator));

                var item = GetItem(queryContext.Documents, key);
                if (item == null)
                    continue;

                if (item.Etag > lastIndexedEtag)
                {
                    DisposeItem();
                    continue;
                }

                if (ItemsAndReferencesAreUsingSameEtagPool && item.Etag > referencedItem.Etag)
                {
                    // if the map worker already mapped this "doc" version it must be with this version of "referencedItem" and if the map worker didn't mapped the "doc" so it will process it later
                    DisposeItem();
                    continue;
                }

                yield return item;

                void DisposeItem()
                {
                    if (item.Item is Document doc)
                        queryContext.Documents.Transaction.ForgetAbout(doc);

                    item.Dispose();
                }
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

        public InMemoryReferencesInfo GetReferencesInfo(TransactionOperationContext indexContext, string collection)
        {
            ReferenceState docs = indexContext.GetReferenceStateFor(ActionType.Document, collection);
            ReferenceState tombstone = indexContext.GetReferenceStateFor(ActionType.Tombstone, collection);
            return new InMemoryReferencesInfo
            {
                ParentItemEtag = docs?.GetLastIndexedParentEtag() ?? 0,
                ParentTombstoneEtag = tombstone?.GetLastIndexedParentEtag() ?? 0
            };
        }

        protected abstract IndexItem GetItem(DocumentsOperationContext databaseContext, Slice key);

        public abstract void HandleDelete(Tombstone tombstone, string collection, Lazy<IndexWriteOperationBase> writer, TransactionOperationContext indexContext, IndexingStatsScope stats);

        public sealed class Reference : IDisposable
        {
            public LazyStringValue Key;

            public long Etag;

            public void Dispose()
            {
                Key?.Dispose();
            }
        }

        public enum ActionType
        {
            Document,
            Tombstone
        }

        public sealed class InMemoryReferencesInfo
        {
            public static InMemoryReferencesInfo Default = new InMemoryReferencesInfo();

            public long ParentItemEtag;
            public long ParentTombstoneEtag;
        }
    }
}
