using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Configuration;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Test;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Indexes.Static
{
    public class MapIndex : MapIndexBase<MapIndexDefinition, IndexField>
    {
        private readonly HashSet<string> _referencedCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> _suggestionsActive = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        protected internal readonly AbstractStaticIndexBase _compiled;
        private bool? _isSideBySide;

        private HandleDocumentReferences _handleReferences;
        private HandleCompareExchangeReferences _handleCompareExchangeReferences;

        protected MapIndex(MapIndexDefinition definition, AbstractStaticIndexBase compiled)
            : base(definition.IndexDefinition.Type, definition.IndexDefinition.SourceType, definition)
        {
            _compiled = compiled;

            foreach (var field in definition.IndexDefinition.Fields)
            {
                var suggestionOption = field.Value.Suggestions;
                if (suggestionOption.HasValue && suggestionOption.Value)
                {
                    _suggestionsActive.Add(field.Key);
                }
            }

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

        public override void ResetIsSideBySideAfterReplacement()
        {
            _isSideBySide = null;
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            var workers = new List<IIndexingWork>
            {
                new CleanupDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration, null)
            };

            if (_compiled.CollectionsWithCompareExchangeReferences.Count > 0)
                workers.Add(_handleCompareExchangeReferences = new HandleCompareExchangeReferences(this, _compiled.CollectionsWithCompareExchangeReferences, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration));

            if (_referencedCollections.Count > 0)
                workers.Add(_handleReferences = new HandleDocumentReferences(this, _compiled.ReferencedCollections, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration));

            workers.Add(new MapDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, null, Configuration));

            return workers.ToArray();
        }

        public override HandleReferencesBase.InMemoryReferencesInfo GetInMemoryReferencesState(string collection, bool isCompareExchange)
        {
            var references = isCompareExchange ? (HandleReferencesBase)_handleCompareExchangeReferences : _handleReferences;
            return references == null ? HandleReferencesBase.InMemoryReferencesInfo.Default : references.GetReferencesInfo(collection);
        }

        public override void HandleDelete(Tombstone tombstone, string collection, Lazy<IndexWriteOperationBase> writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            StaticIndexHelper.HandleReferencesDelete(_handleReferences, _handleCompareExchangeReferences, tombstone, collection, writer, indexContext, stats);

            base.HandleDelete(tombstone, collection, writer, indexContext, stats);
        }

        internal override bool IsStale(QueryOperationContext queryContext, TransactionOperationContext indexContext, long? cutoff = null, long? referenceCutoff = null, long? compareExchangeReferenceCutoff = null, List<string> stalenessReasons = null)
        {
            var isStale = base.IsStale(queryContext, indexContext, cutoff, referenceCutoff, compareExchangeReferenceCutoff, stalenessReasons);
            if (isStale && (stalenessReasons == null || (_handleReferences == null && _handleCompareExchangeReferences == null)))
                return isStale;

            return StaticIndexHelper.IsStaleDueToReferences(this, queryContext, indexContext, referenceCutoff, compareExchangeReferenceCutoff, stalenessReasons) || isStale;
        }

        protected override void HandleDocumentChange(DocumentChange change)
        {
            if (HandleAllDocs == false && Collections.Contains(change.CollectionName) == false &&
                _referencedCollections.Contains(change.CollectionName) == false)
                return;

            _mre.Set();
        }

        public override (ICollection<string> Static, ICollection<string> Dynamic) GetEntriesFields()
        {
            var staticEntries = _compiled.OutputFields.ToHashSet();
            staticEntries.Add(Constants.Documents.Indexing.Fields.DocumentIdFieldName);

            var dynamicEntries = GetDynamicEntriesFields(staticEntries);

            return (staticEntries, dynamicEntries);
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

        protected override bool ShouldReplace()
        {
            return StaticIndexHelper.ShouldReplace(this, ref _isSideBySide);
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

        public override IIndexedItemEnumerator GetMapEnumerator(IEnumerable<IndexItem> items, string collection, TransactionOperationContext indexContext, IndexingStatsScope stats, IndexType type)
        {
            return new StaticIndexItemEnumerator<DynamicBlittableJson>(items, filter: null, _compiled.Maps[collection], collection, stats, type);
        }

        public override Dictionary<string, long> GetLastProcessedTombstonesPerCollection(ITombstoneAware.TombstoneType tombstoneType)
        {
            if (tombstoneType != ITombstoneAware.TombstoneType.Documents)
                return null;

            using (CurrentlyInUse())
            {
                return StaticIndexHelper.GetLastProcessedDocumentTombstonesPerCollection(
                    this, _referencedCollections, Collections, _compiled.ReferencedCollections, _indexStorage);
            }
        }

        public static Index CreateNew(IndexDefinition definition, DocumentDatabase documentDatabase)
        {
            return CreateNew(definition, documentDatabase, new SingleIndexConfiguration(definition.Configuration, documentDatabase.Configuration));
        }
        
        public static Index CreateNewForTest(IndexDefinition definition, DocumentsOperationContext context)
        {
            var index = CreateNew(definition, context.DocumentDatabase, new TestIndexConfiguration(definition.Configuration, context.DocumentDatabase.Configuration));

            index.InitializeTestIndex(context);
            
            return index;
        }
        
        private static Index CreateNew(IndexDefinition definition, DocumentDatabase documentDatabase, SingleIndexConfiguration configuration)
        {
            var instance = CreateIndexInstance(definition, documentDatabase.Configuration, IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion);
            
            var staticIndex = instance._compiled;
            staticIndex.CheckDepthOfStackInOutputMap(definition, documentDatabase);
            
            instance.Initialize(documentDatabase,
                configuration,
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
        
        public static void Update(Index index, IndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var staticMapIndex = (MapIndex)index;
            var staticIndex = staticMapIndex._compiled;

            var staticMapIndexDefinition = new MapIndexDefinition(definition, staticIndex.Maps.Keys, staticIndex.OutputFields, staticIndex.HasDynamicFields, staticIndex.CollectionsWithCompareExchangeReferences.Count > 0, staticMapIndex.Definition.Version);
            staticMapIndex.Update(staticMapIndexDefinition, new SingleIndexConfiguration(definition.Configuration, documentDatabase.Configuration));
        }

        private static MapIndex CreateIndexInstance(IndexDefinition definition, RavenConfiguration configuration, long indexVersion)
        {
            var context = CreateIndexInformationHolder(definition, configuration, indexVersion, out var staticIndex);

            var instance = new MapIndex((MapIndexDefinition)context.Definition, staticIndex);
            return instance;
        }

        public static IndexInformationHolder CreateIndexInformationHolder(IndexDefinition definition, RavenConfiguration configuration, long indexVersion, out AbstractStaticIndexBase staticIndex)
        {
            staticIndex = IndexCompilationCache.GetIndexInstance(definition, configuration, indexVersion);

            var staticMapIndexDefinition = new MapIndexDefinition(definition, staticIndex.Maps.Keys, staticIndex.OutputFields, staticIndex.HasDynamicFields, staticIndex.CollectionsWithCompareExchangeReferences.Count > 0, indexVersion);
            var indexConfiguration = new SingleIndexConfiguration(definition.Configuration, configuration);

            return IndexInformationHolder.CreateFor(staticMapIndexDefinition, indexConfiguration, staticIndex);
        }
    }
}
