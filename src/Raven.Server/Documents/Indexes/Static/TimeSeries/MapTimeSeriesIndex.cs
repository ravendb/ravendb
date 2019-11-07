using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Configuration;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Indexes.Workers.TimeSeries;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Static.TimeSeries
{
    public class MapTimeSeriesIndex : MapIndexBase<MapIndexDefinition, IndexField>
    {
        private readonly HashSet<string> _referencedCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly StaticTimeSeriesIndexBase _compiled;

        private HandleReferences _handleReferences;

        protected MapTimeSeriesIndex(MapIndexDefinition definition, StaticTimeSeriesIndexBase compiled)
            : base(definition.IndexDefinition.Type, definition)
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

        protected override void SubscribeToChanges(DocumentDatabase documentDatabase)
        {
            if (DocumentDatabase != null)
                DocumentDatabase.Changes.OnTimeSeriesChange += HandleTimeSeriesChange;
        }

        protected override void UnsubscribeFromChanges(DocumentDatabase documentDatabase)
        {
            if (DocumentDatabase != null)
                DocumentDatabase.Changes.OnTimeSeriesChange -= HandleTimeSeriesChange;
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            var workers = new List<IIndexingWork>
            {
                //new CleanupDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration, null)
            };

            if (_referencedCollections.Count > 0)
                workers.Add(_handleReferences = new HandleTimeSeriesReferences(this, _compiled.ReferencedCollections, DocumentDatabase.DocumentsStorage.TimeSeriesStorage, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration));

            workers.Add(new MapTimeSeries(this, DocumentDatabase.DocumentsStorage.TimeSeriesStorage, _indexStorage, null, Configuration));

            return workers.ToArray();
        }

        public override void HandleDelete(Tombstone tombstone, IIndexCollection collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            throw new NotSupportedException();

            //if (_referencedCollections.Count > 0)
            //    _handleReferences.HandleDelete(tombstone, collection, writer, indexContext, stats);

            //base.HandleDelete(tombstone, collection, writer, indexContext, stats);
        }

        internal override IEnumerable<IIndexCollection> GetCollectionsForIndexing()
        {
            return _compiled.Maps.Keys;
        }

        public override Dictionary<IIndexCollection, HashSet<CollectionName>> GetReferencedCollections()
        {
            return _compiled.ReferencedCollections;
        }

        public override long GetLastItemEtagInCollection(DocumentsOperationContext databaseContext, IIndexCollection collection)
        {
            if (collection.CollectionName == Constants.Documents.Collections.AllDocumentsCollection)
                throw new InvalidOperationException("TODO ppekrol");

            var timeSeriesCollection = (TimeSeriesCollection)collection;

            // TODO [ppekrol] implement this properly
            var timeSeries = DocumentDatabase.DocumentsStorage.TimeSeriesStorage.GetTimeSeriesFrom(databaseContext, 0)
                .Where(x => string.Equals(timeSeriesCollection.CollectionName, x.Collection, StringComparison.OrdinalIgnoreCase) && string.Equals(timeSeriesCollection.TimeSeriesName, x.Name, StringComparison.OrdinalIgnoreCase))
                .LastOrDefault();

            if (timeSeries == null)
                return 0;

            return timeSeries.Etag;
        }

        public override (ICollection<string> Static, ICollection<string> Dynamic) GetEntriesFields()
        {
            throw new NotImplementedException();
        }

        public override IIndexedItemEnumerator GetMapEnumerator(IEnumerable<IndexItem> items, IIndexCollection collection, TransactionOperationContext indexContext, IndexingStatsScope stats, IndexType type)
        {
            return new StaticIndexItemEnumerator<DynamicTimeSeriesSegment>(items, _compiled.Maps[collection], collection, stats, type);
        }

        public static Index CreateNew(TimeSeriesIndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var instance = CreateIndexInstance(definition, documentDatabase.Configuration);
            instance.Initialize(documentDatabase,
                new SingleIndexConfiguration(definition.Configuration, documentDatabase.Configuration),
                documentDatabase.Configuration.PerformanceHints);

            return instance;
        }

        private static MapTimeSeriesIndex CreateIndexInstance(TimeSeriesIndexDefinition definition, RavenConfiguration configuration)
        {
            var staticIndex = IndexCompilationCache.GetIndexInstance(definition, configuration);

            var staticMapIndexDefinition = new MapIndexDefinition(definition, staticIndex.GetDocumentsCollections(), staticIndex.OutputFields, staticIndex.HasDynamicFields);
            var instance = new MapTimeSeriesIndex(staticMapIndexDefinition, staticIndex);
            return instance;
        }

        private void HandleTimeSeriesChange(TimeSeriesChange change)
        {
            if (HandleAllDocs)
                throw new InvalidOperationException("TODO ppekrol");

            var collection = new TimeSeriesCollection(change.CollectionName, change.Name);

            if (_compiled.Maps.ContainsKey(collection) == false)
                return;

            _mre.Set();
        }
    }
}
