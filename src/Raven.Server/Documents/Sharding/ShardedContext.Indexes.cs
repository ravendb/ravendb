using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch;
using Raven.Server.Extensions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding;

public class ShardedIndexesCache
{
    private readonly ServerStore _serverStore;
    private Dictionary<string, IndexDefinition> _cachedMapReduceIndexDefinitions = new(StringComparer.OrdinalIgnoreCase);
    private Dictionary<string, AutoIndexDefinition> _cachedAutoMapReduceIndexDefinitions = new(StringComparer.OrdinalIgnoreCase);
    private readonly ScriptRunnerCache _scriptRunnerCache;
    private RavenConfiguration _configuration;

    public ScriptRunnerCache ScriptRunnerCache => _scriptRunnerCache;

    public ShardedIndexesCache(ServerStore serverStore, DatabaseRecord record)
    {
        _serverStore = serverStore;

        _configuration = DatabasesLandlord.CreateConfiguration(_serverStore, record.DatabaseName, record.Settings);
        _scriptRunnerCache = new ScriptRunnerCache(database: null, _configuration);

        UpdateConfiguration(record.DatabaseName, record.Settings);

        UpdateMapReduceIndexes(record.Indexes
            .Where(x => x.Value.Type.IsStaticMapReduce())
            .ToDictionary(x => x.Key, x => x.Value));

        UpdateAutoMapReduceIndexes(record.AutoIndexes
            .Where(x => x.Value.Type.IsAutoMapReduce())
            .ToDictionary(x => x.Key, x => x.Value));
    }

    public void Update(RawDatabaseRecord record)
    {
        UpdateConfiguration(record.DatabaseName, record.Settings);
        UpdateMapReduceIndexes(record.MapReduceIndexes());
        UpdateAutoMapReduceIndexes(record.AutoMapReduceIndexes());
    }

    private void UpdateConfiguration(string databaseName, Dictionary<string, string> settings)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "Add a test for updated configuration (for projections)");

        _configuration = DatabasesLandlord.CreateConfiguration(_serverStore, databaseName, settings);
        _scriptRunnerCache.UpdateConfiguration(_configuration);
    }

    private void UpdateMapReduceIndexes(Dictionary<string, IndexDefinition> indexDefinitions)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Major, "handle side-by-side");

        var newDefinitions = new Dictionary<string, IndexDefinition>();

        foreach ((string indexName, IndexDefinition definition) in indexDefinitions)
        {
            Debug.Assert(definition.Type.IsStaticMapReduce());

            newDefinitions[indexName] = definition;
        }

        _cachedMapReduceIndexDefinitions = newDefinitions;
    }

    private void UpdateAutoMapReduceIndexes(Dictionary<string, AutoIndexDefinition> autoIndexDefinitions)
    {
        var newDefinitions = new Dictionary<string, AutoIndexDefinition>();

        foreach ((string indexName, AutoIndexDefinition definition) in autoIndexDefinitions)
        {
            Debug.Assert(definition.Type.IsAutoMapReduce());

            newDefinitions[indexName] = definition;
        }

        _cachedAutoMapReduceIndexDefinitions = newDefinitions;
    }

    public AbstractStaticIndexBase GetCompiledMapReduceIndex(string indexName, TransactionOperationContext context)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal,
            "cache the compiled JavaScript indexes - in a concurrent queue since they are single threaded and are not cached in IndexCompilationCache");

        return _cachedMapReduceIndexDefinitions.TryGetValue(indexName, out var indexDefinition) == false
            ? null
            : IndexCompilationCache.GetIndexInstance(indexDefinition, _configuration, IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion);
    }

    public bool TryGetAutoIndexDefinition(string indexName, out AutoIndexDefinition autoIndexDefinition)
    {
        return _cachedAutoMapReduceIndexDefinitions.TryGetValue(indexName, out autoIndexDefinition);
    }

    public bool IsMapReduceIndex(string indexName)
    {
        return _cachedMapReduceIndexDefinitions.TryGetValue(indexName, out _)
               || _cachedAutoMapReduceIndexDefinitions.TryGetValue(indexName, out _);
    }
}
