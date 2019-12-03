using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Configuration;
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

        protected internal readonly StaticTimeSeriesIndexBase _compiled;

        private HandleReferences _handleReferences;

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

            if (_referencedCollections.Count > 0)
                workers.Add(_handleReferences = new HandleDocumentReferences(this, _compiled.ReferencedCollections, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration));

            workers.Add(new MapTimeSeries(this, DocumentDatabase.DocumentsStorage.TimeSeriesStorage, _indexStorage, MapReduceWorkContext, Configuration));
            workers.Add(new ReduceMapResultsOfStaticIndex(this, _compiled.Reduce, Definition, _indexStorage, DocumentDatabase.Metrics, MapReduceWorkContext));

            return workers.ToArray();
        }

        public override IIndexedItemEnumerator GetMapEnumerator(IEnumerable<IndexItem> items, string collection, TransactionOperationContext indexContext, IndexingStatsScope stats, IndexType type)
        {
            return new StaticIndexItemEnumerator<DynamicTimeSeriesSegment>(items, _compiled.Maps[collection], collection, stats, type);
        }

        public override long GetLastItemEtagInCollection(DocumentsOperationContext databaseContext, string collection)
        {
            if (collection == Constants.Documents.Collections.AllDocumentsCollection)
                throw new InvalidOperationException("TODO arek");

            return DocumentDatabase.DocumentsStorage.TimeSeriesStorage.GetLastTimeSeriesEtag(databaseContext, collection);
        }

        protected override IndexItem GetItemByEtag(DocumentsOperationContext databaseContext, long etag)
        {
            var timeSeries = DocumentDatabase.DocumentsStorage.TimeSeriesStorage.GetTimeSeries(databaseContext, etag);
            if (timeSeries == null)
                return default;

            return new TimeSeriesIndexItem(timeSeries.DocIdAndName, timeSeries.DocIdAndName, timeSeries.DocId, timeSeries.DocId, timeSeries.Etag, timeSeries.Baseline, timeSeries.Name, timeSeries.SegmentSize, timeSeries);
        }

        protected override void SubscribeToChanges(DocumentDatabase documentDatabase)
        {
            if (documentDatabase == null)
                return;

            if (_referencedCollections.Count > 0)
                documentDatabase.Changes.OnDocumentChange += HandleDocumentChange;

            documentDatabase.Changes.OnTimeSeriesChange += HandleTimeSeriesChange;
        }

        protected override void UnsubscribeFromChanges(DocumentDatabase documentDatabase)
        {
            if (documentDatabase == null)
                return;

            if (_referencedCollections.Count > 0)
                documentDatabase.Changes.OnDocumentChange -= HandleDocumentChange;

            documentDatabase.Changes.OnTimeSeriesChange -= HandleTimeSeriesChange;
        }

        protected override void HandleDocumentChange(DocumentChange change)
        {
            if (_referencedCollections.Contains(change.CollectionName) == false)
                return;

            _mre.Set();
        }

        private void HandleTimeSeriesChange(TimeSeriesChange change)
        {
            if (HandleAllDocs)
                throw new InvalidOperationException("TODO ppekrol");

            if (Collections.Contains(change.CollectionName) == false)
                return;

            _mre.Set();
        }

        public static MapReduceTimeSeriesIndex CreateNew(IndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var instance = CreateIndexInstance(definition, documentDatabase.Configuration);
            instance.Initialize(documentDatabase,
                new SingleIndexConfiguration(definition.Configuration, documentDatabase.Configuration),
                documentDatabase.Configuration.PerformanceHints);

            return instance;
        }

        private static MapReduceTimeSeriesIndex CreateIndexInstance(IndexDefinition definition, RavenConfiguration configuration)
        {
            var staticIndex = (StaticTimeSeriesIndexBase)IndexCompilationCache.GetIndexInstance(definition, configuration);

            var staticMapIndexDefinition = new MapReduceIndexDefinition(definition, staticIndex.Maps.Keys.ToHashSet(), staticIndex.OutputFields,
                staticIndex.GroupByFields, staticIndex.HasDynamicFields);
            var instance = new MapReduceTimeSeriesIndex(staticMapIndexDefinition, staticIndex);

            return instance;
        }
    }
}
