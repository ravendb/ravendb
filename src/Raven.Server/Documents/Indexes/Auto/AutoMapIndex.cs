using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.Indexes.Debugging;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Indexes.Workers.Cleanup;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide.Context;
using Voron;
using IndexFieldType = Raven.Server.Documents.Indexes.Debugging.IndexFieldType;

namespace Raven.Server.Documents.Indexes.Auto
{
    internal sealed class AutoMapIndex : MapIndexBase<AutoMapIndexDefinition, AutoIndexField>
    {
        // Auto map index references at most one collection (embeddings), this means single hash set is sufficient
        // We only want to use dictionary to pass data to relevant methods
        private readonly HashSet<string> _referencedCollections = new (StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, HashSet<CollectionName>> _referencedCollectionsDict = new (StringComparer.OrdinalIgnoreCase);
        
        private HandleDocumentReferences _handleReferences;

        private AutoMapIndex(AutoMapIndexDefinition definition)
            : base(IndexType.AutoMap, IndexSourceType.Documents, definition, compiled: null)
        {
        }

        public static AutoMapIndex CreateNew(AutoMapIndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var instance = new AutoMapIndex(definition);
            instance.Initialize(documentDatabase, documentDatabase.Configuration.Indexing, documentDatabase.Configuration.PerformanceHints);

            return instance;
        }

        public static AutoMapIndex Open(StorageEnvironment environment, DocumentDatabase documentDatabase)
        {
            var definition = AutoMapIndexDefinition.Load(environment);
            var instance = new AutoMapIndex(definition);
            instance.Initialize(environment, documentDatabase, documentDatabase.Configuration.Indexing, documentDatabase.Configuration.PerformanceHints);
            definition.DeploymentMode = documentDatabase.Configuration.Indexing.AutoIndexDeploymentMode;

            return instance;
        }

        public override IIndexedItemEnumerator GetMapEnumerator(IEnumerable<IndexItem> items, string collection, TransactionOperationContext indexContext, IndexingStatsScope stats, IndexType type)
        {
            return new AutoIndexDocsEnumerator(items, stats, collection);
        }

        protected override void HandleDocumentChange(DocumentChange change)
        {
            if (HandleAllDocs == false && Collections.Contains(change.CollectionName) == false && _referencedCollections.Contains(change.CollectionName) == false)
                return;
            
            _mre.Set();
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            var workers = new List<IIndexingWork>
            {
                new CleanupDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration, null),
                new MapDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, null, Configuration)
            };

            var usesEmbeddingsGenerationTask = Definition.IndexFields.Values.Any(x => ((AutoVectorOptions)x.Vector)?.EmbeddingsGenerationTaskIdentifier != null);
            
            if (usesEmbeddingsGenerationTask)
            {
                // We only have a single collection for auto map index
                string collection = Collections.First();
                var referencedEmbeddingsCollection = EmbeddingsHelper.GetEmbeddingDocumentCollectionName(collection);

                var referencedEmbeddingsCollectionName = new CollectionName(referencedEmbeddingsCollection);
                _referencedCollections.Add(referencedEmbeddingsCollection);
                _referencedCollectionsDict.Add(collection, new HashSet<CollectionName>() { referencedEmbeddingsCollectionName });
                
                _handleReferences = new HandleDocumentReferences(this, _referencedCollectionsDict, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration);

                workers.Add(_handleReferences);
            }
            
            return workers.ToArray();
        }
        
        internal override bool IsStale(QueryOperationContext queryContext, TransactionOperationContext indexContext, long? cutoff = null, long? referenceCutoff = null, long? compareExchangeReferenceCutoff = null, List<string> stalenessReasons = null)
        {
            var isStale = base.IsStale(queryContext, indexContext, cutoff, referenceCutoff, compareExchangeReferenceCutoff, stalenessReasons);
            if (isStale && (stalenessReasons == null || _handleReferences == null))
                return isStale;

            return StaticIndexHelper.IsStaleDueToReferences(this, queryContext, indexContext, referenceCutoff, compareExchangeReferenceCutoff, stalenessReasons) || isStale;
        }
        
        public override Dictionary<string, HashSet<CollectionName>> GetReferencedCollections()
        {
            return _referencedCollectionsDict;
        }
        
        public override HandleReferencesBase.InMemoryReferencesInfo GetInMemoryReferencesState(string collection, bool isCompareExchange)
        {
            var references = isCompareExchange ? (HandleReferencesBase)null : _handleReferences;
            return references == null ? HandleReferencesBase.InMemoryReferencesInfo.Default : references.GetReferencesInfo(collection);
        }
        
        protected override long CalculateIndexEtag(QueryOperationContext queryContext, TransactionOperationContext indexContext, QueryMetadata query, bool isStale)
        {
            if (_handleReferences == null)
                return base.CalculateIndexEtag(queryContext, indexContext, query, isStale);

            return CalculateIndexEtagWithReferences(
                _handleReferences, null, queryContext,
                indexContext, query, isStale, _referencedCollections, _referencedCollectionsDict, null);
        }
        
        public override Dictionary<string, long> GetLastProcessedTombstonesPerCollection(ITombstoneAware.TombstoneType tombstoneType, Dictionary<string, LastTombstoneInfo> lastProcessedTombstonesInfo = null)
        {
            if (tombstoneType != ITombstoneAware.TombstoneType.Documents)
                return null;

            using (CurrentlyInUse())
            {
                return StaticIndexHelper.GetLastProcessedDocumentTombstonesPerCollection(
                    this, _referencedCollections, Collections, _referencedCollectionsDict, _indexStorage, lastProcessedTombstonesInfo);
            }
        }
        
        public override void HandleDelete(Tombstone tombstone, string collection, Lazy<IndexWriteOperationBase> writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            StaticIndexHelper.HandleReferencesDelete(_handleReferences, null, tombstone, collection, writer, indexContext, stats);

            base.HandleDelete(tombstone, collection, writer, indexContext, stats);
        }

        public override void Update(IndexDefinitionBaseServerSide definition, IndexingConfiguration configuration)
        {
            bool startIndex = UpdateIndexState(definition, true);
            if (startIndex && Status != IndexRunningStatus.Running)
                Start();
        }

        public override void SetState(IndexState state, bool inMemoryOnly = false, bool ignoreWriteError = false)
        {
            base.SetState(state, inMemoryOnly, ignoreWriteError);
            Definition.State = state;
        }

        public override HashSet<FieldDebugInfo> GetEntriesFields()
        {
            var allFields = GetEntriesFields(Definition.IndexFields.Keys);
            allFields.Add(new(Constants.Documents.Indexing.Fields.DocumentIdFieldName, IndexFieldType.Static, IndexedValueType.Term));
            return allFields;
        }

        protected override void LoadValues()
        {
            base.LoadValues();
            Definition.State = State;
        }
    }
}
