using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Indexes.Sharding;

public class ShardedIndexPriorityController : AbstractIndexPriorityController
{
    private readonly ShardedDatabaseContext _context;

    public ShardedIndexPriorityController([NotNull] ShardedDatabaseContext context, ServerStore serverStore)
        : base(serverStore)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
    }

    protected override void ValidateIndex(string name, IndexPriority priority)
    {
        if (_context.Indexes.GetIndex(name) == null)
            IndexDoesNotExistException.ThrowFor(name);
    }

    protected override string GetDatabaseName() => _context.DatabaseName;

    protected override ValueTask WaitForIndexNotificationAsync(long index) => _context.Cluster.WaitForExecutionOfRaftCommandsAsync(index);
}
