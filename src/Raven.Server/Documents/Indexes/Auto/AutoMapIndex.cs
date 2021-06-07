using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config.Categories;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Indexes.Auto
{
    public class AutoMapIndex : MapIndexBase<AutoMapIndexDefinition, AutoIndexField>
    {
        private AutoMapIndex(AutoMapIndexDefinition definition)
            : base(IndexType.AutoMap, IndexSourceType.Documents, definition)
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
            return new AutoIndexDocsEnumerator(items, stats);
        }

        public override void Update(IndexDefinitionBase definition, IndexingConfiguration configuration)
        {
            SetLock(definition.LockMode);
            SetPriority(definition.Priority);
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
