using System;
using Raven.Client.ServerWide;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

public class ClusterHealthPolicy : IServerBackupPolicy
{
    public static readonly ClusterHealthPolicy Instance = new();

    private ClusterHealthPolicy()
    {
    }

    public bool CanDoBackup(ServerStore serverStore, DateTime now, out string reason)
    {
        var state = serverStore.Engine.CurrentCommittedState.State;
        if (state is RachisState.Leader or RachisState.Follower)
        {
            reason = null;
            return true;
        }

        reason = $"Cannot start any backup(s) because cluster is in '{state}' state.";
        return false;
    }
}
