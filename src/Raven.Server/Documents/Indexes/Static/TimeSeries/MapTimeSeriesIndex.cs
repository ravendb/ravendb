using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes.Configuration;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Indexes.Workers.TimeSeries;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Results.TimeSeries;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Indexes.Static.TimeSeries
{
    public class MapTimeSeriesIndex : MapIndexBase<MapIndexDefinition, IndexField>
    {
        private readonly HashSet<string> _referencedCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        protected internal readonly AbstractStaticIndexBase _compiled;
        private bool? _isSideBySide;

        private HandleTimeSeriesReferences _handleReferences;
        private HandleCompareExchangeTimeSeriesReferences _handleCompareExchangeReferences;

        protected MapTimeSeriesIndex(MapIndexDefinition definition, AbstractStaticIndexBase compiled)
            : base(definition.IndexDefinition.Type, definition.IndexDefinition.SourceType, definition)
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

        public override bool HasBoostedFields => _compiled.HasBoostedFields;

        public override bool IsMultiMap => _compiled.Maps.Count > 1 || _compiled.Maps.Any(x => x.Value.Count > 1 || x.Value.Any(y => y.Value.Count > 1));

        public override IQueryResultRetriever GetQueryResultRetriever(IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsOperationContext documentsContext, FieldsToFetch fieldsToFetch, IncludeDocumentsCommand includeDocumentsCommand, IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand)
        {
            return new TimeSeriesQueryResultRetriever(DocumentDatabase, query, queryTimings, DocumentDatabase.DocumentsStorage, documentsContext, fieldsToFetch, includeDocumentsCommand, includeCompareExchangeValuesCommand);
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

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            var workers = new List<IIndexingWork>
            {
                new CleanupDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration, null)
            };

            if (_compiled.CollectionsWithCompareExchangeReferences.Count > 0)
                workers.Add(_handleCompareExchangeReferences = new HandleCompareExchangeTimeSeriesReferences(this, _compiled.CollectionsWithCompareExchangeReferences, DocumentDatabase.DocumentsStorage.TimeSeriesStorage, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration));

            if (_referencedCollections.Count > 0)
                workers.Add(_handleReferences = new HandleTimeSeriesReferences(this, _compiled.ReferencedCollections, DocumentDatabase.DocumentsStorage.TimeSeriesStorage, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration));

            workers.Add(new MapTimeSeries(this, DocumentDatabase.DocumentsStorage.TimeSeriesStorage, _indexStorage, null, Configuration));

            return workers.ToArray();
        }

        public override HandleReferencesBase.InMemoryReferencesInfo GetInMemoryReferencesState(string collection, bool isCompareExchange)
        {
            var references = isCompareExchange ? (HandleReferencesBase)_handleCompareExchangeReferences : _handleReferences;
            return references == null ? HandleReferencesBase.InMemoryReferencesInfo.Default : references.GetReferencesInfo(collection);
        }

        public override void HandleDelete(Tombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            StaticIndexHelper.HandleDeleteBySourceDocument(_handleReferences, _handleCompareExchangeReferences, tombstone, collection, writer, indexContext, stats);
        }

        protected override IndexItem GetItemByEtag(QueryOperationContext queryContext, long etag)
        {
            var timeSeries = DocumentDatabase.DocumentsStorage.TimeSeriesStorage.GetTimeSeries(queryContext.Documents, etag);
            if (timeSeries == null)
                return default;

            return new TimeSeriesIndexItem(timeSeries.LuceneKey, timeSeries.DocId, timeSeries.Etag, timeSeries.Start, timeSeries.Name, timeSeries.SegmentSize, timeSeries);
        }

        protected override bool ShouldReplace()
        {
            return StaticIndexHelper.ShouldReplace(this, ref _isSideBySide);
        }

        public override void ResetIsSideBySideAfterReplacement()
        {
            _isSideBySide = null;
        }

        internal override bool IsStale(QueryOperationContext queryContext, TransactionOperationContext indexContext, long? cutoff = null, long? referenceCutoff = null, long? compareExchangeReferenceCutoff = null, List<string> stalenessReasons = null)
        {
            var isStale = base.IsStale(queryContext, indexContext, cutoff, referenceCutoff, compareExchangeReferenceCutoff, stalenessReasons);
            if (isStale && (stalenessReasons == null || (_handleReferences == null && _handleCompareExchangeReferences == null)))
                return isStale;

            return StaticIndexHelper.IsStaleDueToReferences(this, queryContext, indexContext, referenceCutoff, compareExchangeReferenceCutoff, stalenessReasons) || isStale;
        }

        public override Dictionary<string, HashSet<CollectionName>> GetReferencedCollections()
        {
            return _compiled.ReferencedCollections;
        }

        public override bool WorksOnAnyCollection(HashSet<string> collections)
        {
            if (base.WorksOnAnyCollection(collections))
                return true;

            if (_referencedCollections == null)
                return false;

            return _referencedCollections.Overlaps(collections);
        }

        protected override long CalculateIndexEtag(QueryOperationContext queryContext, TransactionOperationContext indexContext, QueryMetadata query, bool isStale)
        {
            if (_handleReferences == null && _handleCompareExchangeReferences == null)
                return base.CalculateIndexEtag(queryContext, indexContext, query, isStale);

            return CalculateIndexEtagWithReferences(
                _handleReferences, _handleCompareExchangeReferences, queryContext,
                indexContext, query, isStale, _referencedCollections, _compiled);
        }

        protected override IndexingState GetIndexingStateInternal(QueryOperationContext queryContext, TransactionOperationContext indexContext)
        {
            var result = base.GetIndexingStateInternal(queryContext, indexContext);
            if (_handleCompareExchangeReferences == null)
                return result;

            (result.LastProcessedCompareExchangeReferenceEtag, result.LastProcessedCompareExchangeReferenceTombstoneEtag) = StaticIndexHelper.GetLastProcessedCompareExchangeReferenceEtags(this, _compiled, indexContext);

            return result;
        }

        public override long GetLastItemEtagInCollection(QueryOperationContext queryContext, string collection)
        {
            if (collection == Constants.Documents.Collections.AllDocumentsCollection)
                return DocumentDatabase.DocumentsStorage.TimeSeriesStorage.GetLastTimeSeriesEtag(queryContext.Documents);

            return DocumentDatabase.DocumentsStorage.TimeSeriesStorage.GetLastTimeSeriesEtag(queryContext.Documents, collection);
        }

        public override (ICollection<string> Static, ICollection<string> Dynamic) GetEntriesFields()
        {
            var staticEntries = _compiled.OutputFields.ToHashSet();

            var dynamicEntries = GetDynamicEntriesFields(staticEntries);

            return (staticEntries, dynamicEntries);
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

        public static Index CreateNew(IndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var instance = CreateIndexInstance(definition, documentDatabase.Configuration, IndexDefinitionBase.IndexVersion.CurrentVersion);
            instance.Initialize(documentDatabase,
                new SingleIndexConfiguration(definition.Configuration, documentDatabase.Configuration),
                documentDatabase.Configuration.PerformanceHints);

            return instance;
        }

        public static Index Open(StorageEnvironment environment, DocumentDatabase documentDatabase)
        {
            var definition = MapIndexDefinition.Load(environment, out var version);
            var instance = CreateIndexInstance(definition, documentDatabase.Configuration, version);

            instance.Initialize(environment, documentDatabase,
                new SingleIndexConfiguration(definition.Configuration, documentDatabase.Configuration),
                documentDatabase.Configuration.PerformanceHints);

            return instance;
        }

        private static MapTimeSeriesIndex CreateIndexInstance(IndexDefinition definition, RavenConfiguration configuration, long indexVersion)
        {
            var staticIndex = IndexCompilationCache.GetIndexInstance(definition, configuration);

            var staticMapIndexDefinition = new MapIndexDefinition(definition, staticIndex.Maps.Keys, staticIndex.OutputFields, staticIndex.HasDynamicFields, staticIndex.CollectionsWithCompareExchangeReferences.Count > 0, indexVersion);
            var instance = new MapTimeSeriesIndex(staticMapIndexDefinition, staticIndex);
            return instance;
        }

        public static void Update(Index index, IndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var staticMapIndex = (MapTimeSeriesIndex)index;
            var staticIndex = staticMapIndex._compiled;

            var staticMapIndexDefinition = new MapIndexDefinition(definition, staticIndex.Maps.Keys, staticIndex.OutputFields, staticIndex.HasDynamicFields, staticIndex.CollectionsWithCompareExchangeReferences.Count > 0, staticMapIndex.Definition.Version);
            staticMapIndex.Update(staticMapIndexDefinition, new SingleIndexConfiguration(definition.Configuration, documentDatabase.Configuration));
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
