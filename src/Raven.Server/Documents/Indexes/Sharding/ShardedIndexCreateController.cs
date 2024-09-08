using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Handlers.Processors.Collections;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Indexes.Sharding;

public sealed class ShardedIndexCreateController : AbstractIndexCreateController
{
    private readonly ShardedDatabaseContext _context;

    public ShardedIndexCreateController([NotNull] ShardedDatabaseContext context, ServerStore serverStore)
        : base(serverStore)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
    }

    protected override string GetDatabaseName() => _context.DatabaseName;

    public override SystemTime GetDatabaseTime() => _context.Time;

    public override RavenConfiguration GetDatabaseConfiguration() => _context.Configuration;

    protected override IndexInformationHolder GetIndex(string name)
    {
        return _context.Indexes.GetIndex(name);
    }

    protected override IEnumerable<string> GetIndexNames()
    {
        foreach (var index in _context.Indexes.GetIndexes())
            yield return index.Name;
    }

    protected override async ValueTask<long> GetCollectionCountAsync(string collection)
    {
        var op = new ShardedCollectionStatisticsOperation();

        var stats = await _context.ShardExecutor.ExecuteParallelForAllAsync(op);

        return stats.Collections.TryGetValue(collection, out var collectionCount)
            ? collectionCount
            : 0;
    }

    protected override IEnumerable<IndexInformationHolder> GetIndexes() => _context.Indexes.GetIndexes();

    protected override async ValueTask WaitForIndexNotificationAsync(long index, TimeSpan? timeout = null)
    {
        if (timeout != null)
        {
            using var cts = new CancellationTokenSource(timeout.Value);
            await _context.Cluster.WaitForExecutionOnAllNodesAsync(index, cts.Token);
            return;
        }

        await _context.Cluster.WaitForExecutionOnAllNodesAsync(index);
    }

    protected override async ValueTask ValidateStaticIndexAsync(IndexDefinition definition)
    {
        if (definition.DeploymentMode is IndexDeploymentMode.Rolling || 
            definition.DeploymentMode.HasValue == false && _context.Configuration.Indexing.StaticIndexDeploymentMode == IndexDeploymentMode.Rolling)
            throw new NotSupportedInShardingException("Rolling index deployment for a sharded database is currently not supported");

        await base.ValidateStaticIndexAsync(definition);

        if (string.IsNullOrEmpty(definition.OutputReduceToCollection) == false)
            throw new NotSupportedInShardingException("Index with output reduce to collection is not supported in sharding.");
    }

    protected override void ValidateAutoIndex(IndexDefinitionBaseServerSide definition)
    {
        if (definition.DeploymentMode == IndexDeploymentMode.Rolling)
            throw new NotSupportedInShardingException("Rolling index deployment for a sharded database is currently not supported");

        base.ValidateAutoIndex(definition);
    }
}
