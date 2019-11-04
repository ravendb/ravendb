using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Configuration;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Static.TimeSeries
{
    public class MapTimeSeriesIndex : MapIndexBase<MapIndexDefinition, IndexField>
    {
        private readonly StaticTimeSeriesIndexBase _compiled;

        protected MapTimeSeriesIndex(MapIndexDefinition definition, StaticTimeSeriesIndexBase compiled)
            : base(definition.IndexDefinition.Type, definition)
        {
            _compiled = compiled;
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            var workers = new List<IIndexingWork>
            {
                //new CleanupDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration, null)
            };


            workers.Add(new MapTimeSeries(this, DocumentDatabase.DocumentsStorage.TimeSeriesStorage, _indexStorage, null, Configuration));

            return workers.ToArray();
        }

        internal override IEnumerable<IIndexingCollection> GetCollectionsForIndexing()
        {
            return _compiled.Maps.Keys;
        }

        public override long GetLastItemEtagInCollection(DocumentsOperationContext databaseContext, IIndexingCollection collection)
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

        public override IIndexedItemEnumerator GetMapEnumerator(IEnumerable<IndexingItem> items, IIndexingCollection collection, TransactionOperationContext indexContext, IndexingStatsScope stats, IndexType type)
        {
            throw new NotImplementedException();
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
    }
}
