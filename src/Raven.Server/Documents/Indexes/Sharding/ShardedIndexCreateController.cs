using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Indexes.Sharding;

public class ShardedIndexCreateController : AbstractIndexCreateController
{
    private readonly ShardedDatabaseContext _context;

    public ShardedIndexCreateController([NotNull] ShardedDatabaseContext context, ServerStore serverStore)
        : base(serverStore)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
    }

    protected override string GetDatabaseName() => _context.DatabaseName;

    protected override SystemTime GetDatabaseTime() => _context.Time;

    protected override RavenConfiguration GetDatabaseConfiguration() => _context.Configuration;

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
        var op = new ShardedCollectionHandler.ShardedCollectionStatisticsOperation();

        var stats = await _context.ShardExecutor.ExecuteParallelForAllAsync(op);

        return stats.Collections.TryGetValue(collection, out var collectionCount)
            ? collectionCount
            : 0;
    }

    protected override IEnumerable<IndexInformationHolder> GetIndexes() => _context.Indexes.GetIndexes();

    protected override ValueTask WaitForIndexNotificationAsync(long index) => _context.Cluster.WaitForExecutionOnAllNodes(index);
}
