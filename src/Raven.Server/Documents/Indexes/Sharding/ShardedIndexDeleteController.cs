using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Indexes.Sharding;

public class ShardedIndexDeleteController : AbstractIndexDeleteController
{
    private readonly ShardedDatabaseContext _context;

    public ShardedIndexDeleteController([NotNull] ShardedDatabaseContext context, ServerStore serverStore)
        : base(serverStore)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
    }

    protected override string GetDatabaseName() => _context.DatabaseName;

    protected override IndexDefinitionBaseServerSide GetIndexDefinition(string name)
    {
        var index = _context.Indexes.GetIndex(name);
        return index?.Definition;
    }

    protected override async ValueTask CreateIndexAsync(IndexDefinition definition, string raftRequestId)
    {
        await _context.Indexes.Create.CreateIndexAsync(definition, raftRequestId);
    }

    protected override ValueTask WaitForIndexNotificationAsync(long index) => _context.Cluster.WaitForExecutionOnAllNodes(index);
}
