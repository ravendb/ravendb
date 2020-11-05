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
using Raven.Server.Documents.Indexes.Workers.Counters;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Results.Counters;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Indexes.Static.Counters
{
    public class MapCountersIndex : MapIndexBase<MapIndexDefinition, IndexField>
    {
        private readonly HashSet<string> _referencedCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        protected internal readonly StaticCountersIndexBase _compiled;
        private bool? _isSideBySide;

        private HandleCountersReferences _handleReferences;
        private HandleCompareExchangeCountersReferences _handleCompareExchangeReferences;

        protected MapCountersIndex(MapIndexDefinition definition, StaticCountersIndexBase compiled)
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
            return new CountersQueryResultRetriever(DocumentDatabase, query, queryTimings, DocumentDatabase.DocumentsStorage, documentsContext, fieldsToFetch, includeDocumentsCommand, includeCompareExchangeValuesCommand);
        }

        protected override void SubscribeToChanges(DocumentDatabase documentDatabase)
        {
            base.SubscribeToChanges(documentDatabase);

            if (documentDatabase != null)
                documentDatabase.Changes.OnCounterChange += HandleCounterChange;
        }

        protected override void UnsubscribeFromChanges(DocumentDatabase documentDatabase)
        {
            base.UnsubscribeFromChanges(documentDatabase);

            if (documentDatabase != null)
                documentDatabase.Changes.OnCounterChange -= HandleCounterChange;
        }

        protected override void HandleDocumentChange(DocumentChange change)
        {
            if (change.Type == DocumentChangeTypes.Delete && (HandleAllDocs || Collections.Contains(change.CollectionName)))
            {
                // in counters we need to subscribe only to deletions of source documents

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
                workers.Add(_handleCompareExchangeReferences = new HandleCompareExchangeCountersReferences(this, _compiled.CollectionsWithCompareExchangeReferences, DocumentDatabase.DocumentsStorage.CountersStorage, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration));

            if (_referencedCollections.Count > 0)
                workers.Add(_handleReferences = new HandleCountersReferences(this, _compiled.ReferencedCollections, DocumentDatabase.DocumentsStorage.CountersStorage, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration));

            workers.Add(new MapCounters(this, DocumentDatabase.DocumentsStorage.CountersStorage, _indexStorage, null, Configuration));

            return workers.ToArray();
        }

        public override HandleReferencesBase.InMemoryReferencesInfo GetInMemoryReferencesState(string collection)
        {
            if (_handleReferences == null)
                return HandleReferencesBase.InMemoryReferencesInfo.Default;

            return _handleReferences.GetReferencesInfo(collection);
        }

        public override void HandleDelete(Tombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            StaticIndexHelper.HandleDeleteBySourceDocument(_handleReferences, _handleCompareExchangeReferences, tombstone, collection, writer, indexContext, stats);
        }

        protected override IndexItem GetItemByEtag(QueryOperationContext queryContext, long etag)
        {
            var counter = DocumentDatabase.DocumentsStorage.CountersStorage.Indexing.GetCountersMetadata(queryContext.Documents, etag);
            if (counter == null)
                return default;

            return new CounterIndexItem(counter.LuceneKey, counter.DocumentId, counter.Etag, counter.CounterName, counter.Size, counter);
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
            if (isStale && (stalenessReasons == null || _handleReferences == null && _handleCompareExchangeReferences == null))
                return isStale;

            return StaticIndexHelper.IsStaleDueToReferences(this, queryContext, indexContext, referenceCutoff, compareExchangeReferenceCutoff, stalenessReasons) || isStale;
        }

        public override Dictionary<string, HashSet<CollectionName>> GetReferencedCollections()
        {
            return _compiled.ReferencedCollections;
        }

        protected override unsafe long CalculateIndexEtag(QueryOperationContext queryContext, TransactionOperationContext indexContext,
            QueryMetadata query, bool isStale)
        {
            if (_handleReferences == null && _handleCompareExchangeReferences == null)
                return base.CalculateIndexEtag(queryContext, indexContext, query, isStale);

            var minLength = MinimumSizeForCalculateIndexEtagLength(query);
            var length = minLength;

            if (_handleReferences != null)
                length += sizeof(long) * 6 * Collections.Count * _referencedCollections.Count; // last referenced collection etags (document + tombstone) and last processed reference collection etags (document + tombstone)

            if (_handleCompareExchangeReferences != null)
                length += sizeof(long) * 4 * _compiled.CollectionsWithCompareExchangeReferences.Count; // last referenced collection etags (document + tombstone) and last processed reference collection etags (document + tombstone)

            var indexEtagBytes = stackalloc byte[length];

            CalculateIndexEtagInternal(indexEtagBytes, isStale, State, queryContext, indexContext);
            UseAllDocumentsCounterCmpXchgAndTimeSeriesEtags(queryContext, query, length, indexEtagBytes);

            var writePos = indexEtagBytes + minLength;

            return StaticIndexHelper.CalculateIndexEtag(this, length, indexEtagBytes, writePos, queryContext, indexContext);
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
                return DocumentsStorage.ReadLastCountersEtag(queryContext.Documents.Transaction.InnerTransaction);

            return DocumentDatabase.DocumentsStorage.CountersStorage.GetLastCounterEtag(queryContext.Documents, collection);
        }

        public override (ICollection<string> Static, ICollection<string> Dynamic) GetEntriesFields()
        {
            var staticEntries = _compiled.OutputFields.ToHashSet();

            var dynamicEntries = GetDynamicEntriesFields(staticEntries);

            return (staticEntries, dynamicEntries);
        }

        public override IIndexedItemEnumerator GetMapEnumerator(IEnumerable<IndexItem> items, string collection, TransactionOperationContext indexContext, IndexingStatsScope stats, IndexType type)
        {
            return new StaticIndexItemEnumerator<DynamicCounterEntry>(items, new CounterItemFilterBehavior(), _compiled.Maps[collection], collection, stats, type);
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

        private static MapCountersIndex CreateIndexInstance(IndexDefinition definition, RavenConfiguration configuration, long indexVersion)
        {
            var staticIndex = (StaticCountersIndexBase)IndexCompilationCache.GetIndexInstance(definition, configuration);

            var staticMapIndexDefinition = new MapIndexDefinition(definition, staticIndex.Maps.Keys, staticIndex.OutputFields, staticIndex.HasDynamicFields, staticIndex.CollectionsWithCompareExchangeReferences.Count > 0, indexVersion);
            var instance = new MapCountersIndex(staticMapIndexDefinition, staticIndex);
            return instance;
        }

        public static void Update(Index index, IndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var staticMapIndex = (MapCountersIndex)index;
            var staticIndex = staticMapIndex._compiled;

            var staticMapIndexDefinition = new MapIndexDefinition(definition, staticIndex.Maps.Keys, staticIndex.OutputFields, staticIndex.HasDynamicFields, staticIndex.CollectionsWithCompareExchangeReferences.Count > 0, staticMapIndex.Definition.Version);
            staticMapIndex.Update(staticMapIndexDefinition, new SingleIndexConfiguration(definition.Configuration, documentDatabase.Configuration));
        }

        private void HandleCounterChange(CounterChange change)
        {
            if (HandleAllDocs == false && Collections.Contains(change.CollectionName) == false)
                return;

            _mre.Set();
        }
        
        internal override void UpdateProgressStats(QueryOperationContext queryContext, IndexProgress.CollectionStats progressStats, string collectionName)
        {
            progressStats.NumberOfItemsToProcess +=
                DocumentDatabase.DocumentsStorage.CountersStorage.GetNumberOfCounterGroupsToProcess(
                    queryContext.Documents, collectionName, progressStats.LastProcessedItemEtag, out var totalNumberOfItems);
            progressStats.TotalNumberOfItems += totalNumberOfItems;
        }
    }
}
