using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.MapReduce.Workers;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Indexes.Workers.TimeSeries;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Static.TimeSeries
{
    public class MapReduceTimeSeriesIndex : MapReduceIndex
    {
        private readonly HashSet<string> _referencedCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        protected internal new readonly StaticTimeSeriesIndexBase _compiled;

        public MapReduceTimeSeriesIndex(MapReduceIndexDefinition definition, StaticTimeSeriesIndexBase compiled) : base(definition, compiled)
        {
            _compiled = compiled;

            if (_compiled.ReferencedCollections == null)
                return;

            foreach (var collection in _compiled.ReferencedCollections)
            {
                foreach (var referencedCollection in collection.Value)
                    _referencedCollections.Add(referencedCollection.Name);
            }
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            var workers = new List<IIndexingWork>();

            workers.Add(new CleanupDocumentsForMapReduce(this, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration, MapReduceWorkContext));

            if (_compiled.CollectionsWithCompareExchangeReferences.Count > 0)
                workers.Add(_handleCompareExchangeReferences = new HandleCompareExchangeTimeSeriesReferences(this, _compiled.CollectionsWithCompareExchangeReferences, DocumentDatabase.DocumentsStorage.TimeSeriesStorage, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration));

            if (_referencedCollections.Count > 0)
                workers.Add(_handleReferences = new HandleTimeSeriesReferences(this, _compiled.ReferencedCollections, DocumentDatabase.DocumentsStorage.TimeSeriesStorage, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration));

            workers.Add(new MapTimeSeries(this, DocumentDatabase.DocumentsStorage.TimeSeriesStorage, _indexStorage, MapReduceWorkContext, Configuration));
            workers.Add(new ReduceMapResultsOfStaticIndex(this, _compiled.Reduce, Definition, _indexStorage, DocumentDatabase.Metrics, MapReduceWorkContext));

            return workers.ToArray();
        }

        public override void HandleDelete(Tombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            StaticIndexHelper.HandleDeleteBySourceDocumentId(this, _handleReferences, _handleCompareExchangeReferences, tombstone, collection, writer, indexContext, stats);
        }

        public override IIndexedItemEnumerator GetMapEnumerator(IEnumerable<IndexItem> items, string collection, TransactionOperationContext indexContext, IndexingStatsScope stats, IndexType type)
        {
            return new StaticIndexItemEnumerator<DynamicTimeSeriesSegment>(items, filter: null, _compiled.Maps[collection], collection, stats, type);
        }

        public override Dictionary<string, long> GetLastProcessedTombstonesPerCollection(ITombstoneAware.TombstoneType tombstoneType)
        {
            if (tombstoneType == ITombstoneAware.TombstoneType.Documents)
            {
                using (CurrentlyInUse())
                {
                    return StaticIndexHelper.GetLastProcessedDocumentTombstonesPerCollection(
                        this, _referencedCollections, Collections, _compiled.ReferencedCollections, _indexStorage);
                }
            }

            if (tombstoneType == ITombstoneAware.TombstoneType.TimeSeries)
            {
                using (CurrentlyInUse())
                {
                    return StaticIndexHelper.GetLastProcessedEtagsPerCollection(this, Collections, _indexStorage);
                }
            }

            return null;
        }

        public override int HandleMap(IndexItem indexItem, IEnumerable mapResults, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            if (_enumerationWrappers.TryGetValue(CurrentIndexingScope.Current.SourceCollection, out AnonymousObjectToBlittableMapResultsEnumerableWrapper wrapper) == false)
            {
                _enumerationWrappers[CurrentIndexingScope.Current.SourceCollection] = wrapper = new AnonymousObjectToBlittableMapResultsEnumerableWrapper(this, indexContext);
            }

            wrapper.InitializeForEnumeration(mapResults, indexContext, stats);

            return PutMapResults(indexItem.LowerId, indexItem.SourceDocumentId, wrapper, indexContext, stats);
        }

        public override long GetLastItemEtagInCollection(QueryOperationContext queryContext, string collection)
        {
            if (collection == Constants.Documents.Collections.AllDocumentsCollection)
                return DocumentDatabase.DocumentsStorage.TimeSeriesStorage.GetLastTimeSeriesEtag(queryContext.Documents);

            return DocumentDatabase.DocumentsStorage.TimeSeriesStorage.GetLastTimeSeriesEtag(queryContext.Documents, collection);
        }

        protected override IndexItem GetItemByEtag(QueryOperationContext queryContext, long etag)
        {
            var timeSeries = DocumentDatabase.DocumentsStorage.TimeSeriesStorage.GetTimeSeries(queryContext.Documents, etag);
            if (timeSeries == null)
                return default;

            return new TimeSeriesIndexItem(timeSeries.LuceneKey, timeSeries.DocId, timeSeries.Etag, timeSeries.Start, timeSeries.Name, timeSeries.SegmentSize, timeSeries);
        }

        protected override void SubscribeToChanges(DocumentDatabase documentDatabase)
        {
            base.SubscribeToChanges(documentDatabase);

            if (documentDatabase != null)
                documentDatabase.Changes.OnTimeSeriesChange += HandleTimeSeriesChange;
        }

        protected override void UnsubscribeFromChanges(DocumentDatabase documentDatabase)
        {
            base.UnsubscribeFromChanges(documentDatabase);

            if (documentDatabase != null)
                documentDatabase.Changes.OnTimeSeriesChange -= HandleTimeSeriesChange;
        }

        protected override void HandleDocumentChange(DocumentChange change)
        {
            if (change.Type == DocumentChangeTypes.Delete && (HandleAllDocs || Collections.Contains(change.CollectionName)))
            {
                // in time series we need to subscribe only to deletions of source documents

                _mre.Set();
                return;
            }

            if (_referencedCollections.Contains(change.CollectionName))
            {
                _mre.Set();
            }
        }

        private void HandleTimeSeriesChange(TimeSeriesChange change)
        {
            if (HandleAllDocs == false && Collections.Contains(change.CollectionName) == false)
                return;

            _mre.Set();
        }
        
        internal override void UpdateProgressStats(QueryOperationContext queryContext, IndexProgress.CollectionStats progressStats, string collectionName)
        {
            progressStats.NumberOfItemsToProcess +=
                DocumentDatabase.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesSegmentsToProcess(
                    queryContext.Documents, collectionName, progressStats.LastProcessedItemEtag, out var totalCount);
            progressStats.TotalNumberOfItems += totalCount;
        }
    }
}
