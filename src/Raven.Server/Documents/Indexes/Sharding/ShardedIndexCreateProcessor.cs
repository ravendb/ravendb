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
        throw new System.NotImplementedException();
    }

    protected override IEnumerable<string> GetIndexNames()
    {
        throw new System.NotImplementedException();
    }

    protected override async ValueTask WaitForIndexNotificationAsync(long index)
    {
        await ServerStore.Cluster.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);
    }
}
