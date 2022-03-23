using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Indexes.Sharding;

public class ShardedIndexPriorityProcessor : AbstractIndexPriorityProcessor
{
    private readonly ShardedDatabaseContext _context;

    public ShardedIndexPriorityProcessor([NotNull] ShardedDatabaseContext context, ServerStore serverStore)
        : base(serverStore)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
    }

    protected override void ValidateIndex(string name, IndexPriority priority)
    {
        if (_context.Indexes.TryGetIndexDefinition(name, out var indexDefinition) == false)
            IndexDoesNotExistException.ThrowFor(name);
    }

    protected override string GetDatabaseName()
    {
        return _context.DatabaseName;
    }

    protected override async ValueTask WaitForIndexNotificationAsync(long index)
    {
        await ServerStore.Cluster.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);
    }
}
