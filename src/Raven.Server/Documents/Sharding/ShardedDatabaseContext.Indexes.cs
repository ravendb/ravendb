using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Analysis;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Sharding;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch;
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding;

public partial class ShardedDatabaseContext
{
    public readonly ShardedIndexesContext Indexes;

    public sealed class ShardedIndexesContext
    {
        private readonly RavenLogger _logger;

        public readonly ShardedDatabaseContext DatabaseContext;

        private Dictionary<string, IndexInformationHolder> _indexes;

        public readonly ScriptRunnerCache ScriptRunnerCache;

        public readonly ShardedIndexLockModeController LockMode;

        public readonly ShardedIndexPriorityController Priority;

        public readonly ShardedIndexStateController State;

        public readonly ShardedIndexDeleteController Delete;

        public readonly ShardedIndexCreateController Create;

        public ShardedIndexHasChangedController HasChanged;

        public ShardedIndexesContext([NotNull] ShardedDatabaseContext context, ServerStore serverStore)
        {
            DatabaseContext = context ?? throw new ArgumentNullException(nameof(context));
            _logger = RavenLogManager.Instance.GetLoggerForDatabase<ShardedIndexesContext>(context.DatabaseName);

            LockMode = new ShardedIndexLockModeController(context, serverStore);
            Priority = new ShardedIndexPriorityController(context, serverStore);
            State = new ShardedIndexStateController(context, serverStore);
            Delete = new ShardedIndexDeleteController(context, serverStore);
            Create = new ShardedIndexCreateController(context, serverStore);
            HasChanged = new ShardedIndexHasChangedController(context);

            ScriptRunnerCache = new ScriptRunnerCache(database: null, context.Configuration);

            Update(context.DatabaseRecord, 0);
        }

        public void Update(RawDatabaseRecord record, long index)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "RavenDB-19087 Add a test for updated configuration (for projections)");
            ScriptRunnerCache.UpdateConfiguration(DatabaseContext.Configuration);

            UpdateAnalyzers(record, index);

            var indexes = new Dictionary<string, IndexInformationHolder>(StringComparer.OrdinalIgnoreCase);

            UpdateStaticIndexes(record.Indexes, indexes);
            UpdateAutoIndexes(record.AutoIndexes, indexes);

            _indexes = indexes;
        }

        private void UpdateStaticIndexes(Dictionary<string, IndexDefinition> indexDefinitions, Dictionary<string, IndexInformationHolder> indexes)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Major, "RavenDB-19087 handle side-by-side");

            foreach ((string indexName, IndexDefinition definition) in indexDefinitions)
            {
                IndexInformationHolder indexInformationHolder = null;

                if (_indexes != null && _indexes.TryGetValue(indexName, out var existingIndex))
                {
                    var creationOptions = IndexStore.GetIndexCreationOptions(definition, existingIndex, DatabaseContext.Configuration, out _);
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
                    case IndexType.JavaScriptMap:
                        indexInformationHolder = MapIndex.CreateIndexInformationHolder(definition, DatabaseContext.Configuration, IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion, out _);
                        break;
                    case IndexType.MapReduce:
                    case IndexType.JavaScriptMapReduce:
                        indexInformationHolder = MapReduceIndex.CreateIndexInformationHolder(definition, DatabaseContext.Configuration, IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion, out _);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(definition.Type), definition.Type, "Unknown index type");
                }

                return indexInformationHolder;
            }
        }

        private void UpdateAutoIndexes(Dictionary<string, AutoIndexDefinition> indexDefinitions, Dictionary<string, IndexInformationHolder> indexes)
        {
            foreach ((string indexName, AutoIndexDefinition definition) in indexDefinitions)
            {
                var indexDefinition = IndexStore.CreateAutoDefinition(definition, DatabaseContext.Configuration.Indexing.AutoIndexDeploymentMode);

                IndexInformationHolder indexInformationHolder = null;

                if (_indexes != null && _indexes.TryGetValue(indexName, out var existingIndex))
                {
                    var creationOptions = IndexStore.GetIndexCreationOptions(indexDefinition, existingIndex, DatabaseContext.Configuration, out _);
                    if (creationOptions == IndexCreationOptions.Noop)
                        indexInformationHolder = existingIndex;
                }

                indexInformationHolder ??= IndexInformationHolder.CreateFor(indexDefinition, DatabaseContext.Configuration.Indexing);

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


        private void UpdateAnalyzers(RawDatabaseRecord record, long index)
        {
            try
            {
                AnalyzerCompilationCache.Instance.AddItems(record);
            }
            catch (Exception e)
            {
                DatabaseContext.RachisLogIndexNotifications.NotifyListenersAbout(index, e);
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Could not update analyzers", e);
            }
        }
    }

}
