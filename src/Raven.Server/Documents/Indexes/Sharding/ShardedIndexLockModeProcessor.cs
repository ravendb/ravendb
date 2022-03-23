using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Indexes.Sharding;

public class ShardedIndexLockModeProcessor : AbstractIndexLockModeProcessor
{
    private readonly ShardedDatabaseContext _context;

    public ShardedIndexLockModeProcessor([NotNull] ShardedDatabaseContext context, ServerStore serverStore)
        : base(serverStore)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
    }

    protected override void ValidateIndex(string name, IndexLockMode mode)
    {
        if (_context.Indexes.TryGetIndexDefinition(name, out var indexDefinition) == false)
            IndexDoesNotExistException.ThrowFor(name);

        if (indexDefinition is AutoIndexDefinition)
            throw new NotSupportedException($"'Lock Mode' can't be set for the Auto-Index '{name}'.");
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
