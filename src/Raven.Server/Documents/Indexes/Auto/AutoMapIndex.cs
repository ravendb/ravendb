using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Indexes.Workers.Cleanup;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Indexes.Auto
{
    internal sealed class AutoMapIndex : MapIndexBase<AutoMapIndexDefinition, AutoIndexField>
    {
        private readonly Dictionary<string, HashSet<CollectionName>> _referencedCollections = new (StringComparer.OrdinalIgnoreCase);

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
            var collectionName = new CollectionName(change.CollectionName);
            
            if (HandleAllDocs == false && Collections.Contains(change.CollectionName) == false && _referencedCollections.First().Value.Contains(collectionName) == false)
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
                
                _referencedCollections.Add(collection, new HashSet<CollectionName>() { new CollectionName(referencedEmbeddingsCollection) });
                
                workers.Add(new HandleDocumentReferences(this, _referencedCollections, DocumentDatabase.DocumentsStorage, _indexStorage, Configuration));
            }
            
            return workers.ToArray();
        }
        
        internal override bool IsStale(QueryOperationContext queryContext, TransactionOperationContext indexContext, long? cutoff = null, long? referenceCutoff = null, long? compareExchangeReferenceCutoff = null, List<string> stalenessReasons = null)
        {
            var isStale = base.IsStale(queryContext, indexContext, cutoff, referenceCutoff, compareExchangeReferenceCutoff, stalenessReasons);
            if (isStale && stalenessReasons == null)
                return isStale;
            
            if (_referencedCollections.Count == 0)
                return isStale;

            return StaticIndexHelper.IsStaleDueToReferences(this, queryContext, indexContext, referenceCutoff, compareExchangeReferenceCutoff, stalenessReasons) || isStale;
        }
        
        public override Dictionary<string, HashSet<CollectionName>> GetReferencedCollections()
        {
            return _referencedCollections;
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

        public override (ICollection<string> Static, ICollection<string> Dynamic) GetEntriesFields()
        {
            var staticEntries = Definition
                .IndexFields
                .Keys
                .ToHashSet();

            var dynamicEntries = GetDynamicEntriesFields(staticEntries);

            staticEntries.Add(Constants.Documents.Indexing.Fields.DocumentIdFieldName);

            return (staticEntries, dynamicEntries);
        }

        protected override void LoadValues()
        {
            base.LoadValues();
            Definition.State = State;
        }
    }
}
