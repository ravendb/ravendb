using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Indexes;

namespace Raven.Server.Documents.Indexes;

public abstract class AbstractIndexLockModeController
{
    protected readonly ServerStore ServerStore;

    protected AbstractIndexLockModeController([NotNull] ServerStore serverStore)
    {
        ServerStore = serverStore ?? throw new ArgumentNullException(nameof(serverStore));
    }

    protected abstract void ValidateIndex(string name, IndexLockMode mode);

    protected abstract string GetDatabaseName();

    protected abstract ValueTask WaitForIndexNotificationAsync(long index);

    public async Task SetLockAsync(string name, IndexLockMode mode, string raftRequestId)
    {
        ValidateIndex(name, mode);

        var databaseName = GetDatabaseName();

        var command = new SetIndexLockCommand(name, mode, databaseName, raftRequestId);

        var (index, _) = await ServerStore.SendToLeaderAsync(command);

        await WaitForIndexNotificationAsync(index);
    }
}
