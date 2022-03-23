using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Indexes.Sharding;

public class ShardedIndexCreateProcessor : AbstractIndexCreateProcessor
{
    private readonly ShardedDatabaseContext _context;

    public ShardedIndexCreateProcessor([NotNull] ShardedDatabaseContext context, ServerStore serverStore)
        : base(serverStore)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
    }

    protected override string GetDatabaseName() => _context.DatabaseName;

    protected override SystemTime GetDatabaseTime() => _context.Time;

    protected override RavenConfiguration GetDatabaseConfiguration() => _context.Configuration;

    protected override IndexContext GetIndex(string name)
    {
        return _context.Indexes.GetIndex(name);
    }

    protected override IEnumerable<string> GetIndexNames()
    {
        foreach (var index in _context.Indexes.GetIndexes())
            yield return index.Name;
    }

    protected override async ValueTask WaitForIndexNotificationAsync(long index)
    {
        await ServerStore.Cluster.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);
    }
}
