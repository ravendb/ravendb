using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Sharding;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding;

public partial class ShardedDatabaseContext
{
    public readonly ShardedIndexesContext Indexes;

    public class ShardedIndexesContext
    {
        private readonly ShardedDatabaseContext _context;
        private Dictionary<string, IndexContext> _indexes = new(StringComparer.OrdinalIgnoreCase);
        private readonly ScriptRunnerCache _scriptRunnerCache;

        public ScriptRunnerCache ScriptRunnerCache => _scriptRunnerCache;

        public readonly ShardedIndexLockModeProcessor LockMode;

        public readonly ShardedIndexPriorityProcessor Priority;

        public readonly ShardedIndexStateProcessor State;

        public readonly ShardedIndexDeleteProcessor Delete;

        public readonly ShardedIndexCreateProcessor Create;

        public ShardedIndexesContext([NotNull] ShardedDatabaseContext context, ServerStore serverStore)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));

            LockMode = new ShardedIndexLockModeProcessor(context, serverStore);
            Priority = new ShardedIndexPriorityProcessor(context, serverStore);
            State = new ShardedIndexStateProcessor(context, serverStore);
            Delete = new ShardedIndexDeleteProcessor(context, serverStore);
            Create = new ShardedIndexCreateProcessor(context, serverStore);

            _scriptRunnerCache = new ScriptRunnerCache(database: null, context.Configuration);

            var indexes = new Dictionary<string, IndexContext>(StringComparer.OrdinalIgnoreCase);

            UpdateStaticIndexes(context.DatabaseRecord.Indexes
                .ToDictionary(x => x.Key, x => x.Value), indexes);

            UpdateAutoIndexes(context.DatabaseRecord.AutoIndexes
                .ToDictionary(x => x.Key, x => x.Value), indexes);

            _indexes = indexes;
        }

        public void Update(RawDatabaseRecord record)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "Add a test for updated configuration (for projections)");
            _scriptRunnerCache.UpdateConfiguration(_context.Configuration);

            var indexes = new Dictionary<string, IndexContext>(StringComparer.OrdinalIgnoreCase);

            UpdateStaticIndexes(record.Indexes, indexes);
            UpdateAutoIndexes(record.AutoIndexes, indexes);

            _indexes = indexes;
        }

        private void UpdateStaticIndexes(Dictionary<string, IndexDefinition> indexDefinitions, Dictionary<string, IndexContext> indexes)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Major, "handle side-by-side");

            foreach ((string indexName, IndexDefinition definition) in indexDefinitions)
            {
                IndexContext indexContext;
                switch (definition.Type)
                {
                    case IndexType.Map:
                        indexContext = MapIndex.CreateContext(definition, _context.Configuration, IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion, out _);
                        break;
                    case IndexType.MapReduce:
                        indexContext = MapReduceIndex.CreateContext(definition, _context.Configuration, IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion, out _);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                indexes[indexName] = indexContext;
            }
        }

        private void UpdateAutoIndexes(Dictionary<string, AutoIndexDefinition> indexDefinitions, Dictionary<string, IndexContext> indexes)
        {
            foreach ((string indexName, AutoIndexDefinition definition) in indexDefinitions)
            {
                var indexDefinition = IndexStore.CreateAutoDefinition(definition, _context.Configuration.Indexing.AutoIndexDeploymentMode);
                indexes[indexName] = new IndexContext(indexDefinition, _context.Configuration.Indexing);
            }
        }

        public AbstractStaticIndexBase GetCompiledMapReduceIndex(string indexName, TransactionOperationContext context)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal,
                "cache the compiled JavaScript indexes - in a concurrent queue since they are single threaded and are not cached in IndexCompilationCache");

            if (_indexes.TryGetValue(indexName, out var index) == false)
                return null;

            if (index.Type.IsStaticMapReduce() == false)
                throw new InvalidOperationException($"Index '{indexName}' is not a map-reduce index");

            var indexDefinition = (MapReduceIndexDefinition)index.Definition;

            return IndexCompilationCache.GetIndexInstance(indexDefinition.IndexDefinition, _context.Configuration, IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion);
        }

        public IndexContext GetIndex(string name)
        {
            return _indexes.TryGetValue(name, out var index)
                ? index
                : null;
        }

        public IEnumerable<IndexContext> GetIndexes() => _indexes.Values;
    }

}
