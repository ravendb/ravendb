using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Indexes;

namespace Raven.Server.Documents.Indexes;

public abstract class AbstractIndexPriorityController
{
    protected readonly ServerStore ServerStore;

    protected AbstractIndexPriorityController([NotNull] ServerStore serverStore)
    {
        ServerStore = serverStore ?? throw new ArgumentNullException(nameof(serverStore));
    }

    protected abstract void ValidateIndex(string name, IndexPriority priority);

    protected abstract string GetDatabaseName();

    protected abstract ValueTask WaitForIndexNotificationAsync(long index);

    public async Task SetPriorityAsync(string name, IndexPriority priority, string raftRequestId)
    {
        ValidateIndex(name, priority);

        var databaseName = GetDatabaseName();

        var command = new SetIndexPriorityCommand(name, priority, databaseName, raftRequestId);

        var (index, _) = await ServerStore.SendToLeaderAsync(command);

        await WaitForIndexNotificationAsync(index);
    }
}
