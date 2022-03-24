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
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding;

public partial class ShardedDatabaseContext
{
    public readonly ShardedIndexesContext Indexes;

    public class ShardedIndexesContext
    {
        private readonly ShardedDatabaseContext _context;
        private Dictionary<string, IndexInformationHolder> _indexes;

        public readonly ScriptRunnerCache ScriptRunnerCache;

        public readonly ShardedIndexLockModeController LockMode;

        public readonly ShardedIndexPriorityController Priority;

        public readonly ShardedIndexStateController State;

        public readonly ShardedIndexDeleteController Delete;

        public readonly ShardedIndexCreateController Create;

        public ShardedIndexesContext([NotNull] ShardedDatabaseContext context, ServerStore serverStore)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));

            LockMode = new ShardedIndexLockModeController(context, serverStore);
            Priority = new ShardedIndexPriorityController(context, serverStore);
            State = new ShardedIndexStateController(context, serverStore);
            Delete = new ShardedIndexDeleteController(context, serverStore);
            Create = new ShardedIndexCreateController(context, serverStore);

            ScriptRunnerCache = new ScriptRunnerCache(database: null, context.Configuration);

            var indexes = new Dictionary<string, IndexInformationHolder>(StringComparer.OrdinalIgnoreCase);

            UpdateStaticIndexes(context.DatabaseRecord.Indexes
                .ToDictionary(x => x.Key, x => x.Value), indexes);

            UpdateAutoIndexes(context.DatabaseRecord.AutoIndexes
                .ToDictionary(x => x.Key, x => x.Value), indexes);

            _indexes = indexes;
        }

        public void Update(RawDatabaseRecord record)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "Add a test for updated configuration (for projections)");
            ScriptRunnerCache.UpdateConfiguration(_context.Configuration);

            var indexes = new Dictionary<string, IndexInformationHolder>(StringComparer.OrdinalIgnoreCase);

            UpdateStaticIndexes(record.Indexes, indexes);
            UpdateAutoIndexes(record.AutoIndexes, indexes);

            _indexes = indexes;
        }

        private void UpdateStaticIndexes(Dictionary<string, IndexDefinition> indexDefinitions, Dictionary<string, IndexInformationHolder> indexes)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Major, "handle side-by-side");

            foreach ((string indexName, IndexDefinition definition) in indexDefinitions)
            {
                IndexInformationHolder indexInformationHolder = null;

                if (_indexes.TryGetValue(indexName, out var existingIndex))
                {
                    var creationOptions = IndexStore.GetIndexCreationOptions(definition, existingIndex, _context.Configuration, out _);
                    if (creationOptions == IndexCreationOptions.Noop)
                        indexInformationHolder = existingIndex;
                }

                indexInformationHolder ??= CreateContext(definition);

                indexes[indexName] = indexInformationHolder;
            }

            IndexInformationHolder CreateContext(IndexDefinition definition)
            {
                IndexInformationHolder indexInformationHolder;
                switch (definition.Type)
                {
                    case IndexType.Map:
                        indexInformationHolder = MapIndex.CreateIndexInformationHolder(definition, _context.Configuration, IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion, out _);
                        break;
                    case IndexType.MapReduce:
                        indexInformationHolder = MapReduceIndex.CreateIndexInformationHolder(definition, _context.Configuration, IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion, out _);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(definition.Type));
                }

                return indexInformationHolder;
            }
        }

        private void UpdateAutoIndexes(Dictionary<string, AutoIndexDefinition> indexDefinitions, Dictionary<string, IndexInformationHolder> indexes)
        {
            foreach ((string indexName, AutoIndexDefinition definition) in indexDefinitions)
            {
                var indexDefinition = IndexStore.CreateAutoDefinition(definition, _context.Configuration.Indexing.AutoIndexDeploymentMode);

                IndexInformationHolder indexInformationHolder = null;

                if (_indexes.TryGetValue(indexName, out var existingIndex))
                {
                    var creationOptions = IndexStore.GetIndexCreationOptions(indexDefinition, existingIndex, _context.Configuration, out _);
                    if (creationOptions == IndexCreationOptions.Noop)
                        indexInformationHolder = existingIndex;
                }

                indexInformationHolder ??= IndexInformationHolder.CreateFor(indexDefinition, _context.Configuration.Indexing);

                indexes[indexName] = indexInformationHolder;
            }
        }

        public IndexInformationHolder GetIndex(string name)
        {
            return _indexes.TryGetValue(name, out var index)
                ? index
                : null;
        }

        public IEnumerable<IndexInformationHolder> GetIndexes() => _indexes.Values;
    }

}
