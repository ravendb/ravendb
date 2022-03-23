using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Sharding;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding;

public partial class ShardedDatabaseContext
{
    public readonly ShardedIndexesCache Indexes;

    public class ShardedIndexesCache
    {
        private readonly ShardedDatabaseContext _context;
        private Dictionary<string, IndexDefinition> _cachedStaticIndexDefinitions = new(StringComparer.OrdinalIgnoreCase);
        private Dictionary<string, AutoIndexDefinition> _cachedAutoIndexDefinitions = new(StringComparer.OrdinalIgnoreCase);
        private readonly ScriptRunnerCache _scriptRunnerCache;

        public ScriptRunnerCache ScriptRunnerCache => _scriptRunnerCache;

        public readonly ShardedIndexLockModeProcessor LockMode;

        public readonly ShardedIndexPriorityProcessor Priority;

        public readonly ShardedIndexStateProcessor State;

        public readonly ShardedIndexDeleteProcessor Delete;

        public ShardedIndexesCache([NotNull] ShardedDatabaseContext context, ServerStore serverStore)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));

            LockMode = new ShardedIndexLockModeProcessor(context, serverStore);
            Priority = new ShardedIndexPriorityProcessor(context, serverStore);
            State = new ShardedIndexStateProcessor(context, serverStore);
            Delete = new ShardedIndexDeleteProcessor(context, serverStore);

            _scriptRunnerCache = new ScriptRunnerCache(database: null, context.Configuration);

            UpdateStaticIndexes(context.DatabaseRecord.Indexes
                .ToDictionary(x => x.Key, x => x.Value));

            UpdateAutoIndexes(context.DatabaseRecord.AutoIndexes
                .ToDictionary(x => x.Key, x => x.Value));
        }

        public void Update(RawDatabaseRecord record)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "Add a test for updated configuration (for projections)");
            _scriptRunnerCache.UpdateConfiguration(_context.Configuration);

            UpdateStaticIndexes(record.Indexes);
            UpdateAutoIndexes(record.AutoIndexes);
        }

        private void UpdateStaticIndexes(Dictionary<string, IndexDefinition> indexDefinitions)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Major, "handle side-by-side");

            var newDefinitions = new Dictionary<string, IndexDefinition>();

            foreach ((string indexName, IndexDefinition definition) in indexDefinitions)
            {
                newDefinitions[indexName] = definition;
            }

            _cachedStaticIndexDefinitions = newDefinitions;
        }

        private void UpdateAutoIndexes(Dictionary<string, AutoIndexDefinition> indexDefinitions)
        {
            var newDefinitions = new Dictionary<string, AutoIndexDefinition>();

            foreach ((string indexName, AutoIndexDefinition definition) in indexDefinitions)
            {
                newDefinitions[indexName] = definition;
            }

            _cachedAutoIndexDefinitions = newDefinitions;
        }

        public AbstractStaticIndexBase GetCompiledMapReduceIndex(string indexName, TransactionOperationContext context)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal,
                "cache the compiled JavaScript indexes - in a concurrent queue since they are single threaded and are not cached in IndexCompilationCache");

            if (_cachedStaticIndexDefinitions.TryGetValue(indexName, out var indexDefinition) == false)
                return null;

            if (indexDefinition.Type.IsMapReduce() == false)
                throw new InvalidOperationException($"Index '{indexName}' is not a map-reduce index");

            return IndexCompilationCache.GetIndexInstance(indexDefinition, _context.Configuration, IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion);
        }

        public bool TryGetAutoMapReduceIndexDefinition(string indexName, out AutoIndexDefinition autoIndexDefinition)
        {
            if (_cachedAutoIndexDefinitions.TryGetValue(indexName, out autoIndexDefinition) == false)
                return false;

            if (autoIndexDefinition.Type.IsMapReduce() == false)
                throw new InvalidOperationException($"Index '{indexName}' is not a map-reduce index");

            return true;
        }

        public bool IsMapReduceIndex(string indexName)
        {
            if (_cachedStaticIndexDefinitions.TryGetValue(indexName, out var staticIndexDefinition))
                return staticIndexDefinition.Type.IsMapReduce();

            if (_cachedAutoIndexDefinitions.TryGetValue(indexName, out var autoIndexDefinition))
                return autoIndexDefinition.Type.IsMapReduce();

            return false;
        }

        public bool TryGetIndexDefinition(string indexName, out IndexDefinitionBase indexDefinition)
        {
            if (_cachedStaticIndexDefinitions.TryGetValue(indexName, out var staticIndexDefinition))
            {
                indexDefinition = staticIndexDefinition;
                return true;
            }

            if (_cachedAutoIndexDefinitions.TryGetValue(indexName, out var autoIndexDefinition))
            {
                indexDefinition = autoIndexDefinition;
                return true;
            }

            indexDefinition = null;
            return false;
        }
    }

}
