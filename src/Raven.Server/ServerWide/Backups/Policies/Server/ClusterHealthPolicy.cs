using System;
using Raven.Client.ServerWide;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

/// <summary>
/// Blocks all backups when this node is not in a stable cluster role (Leader or Follower).
/// Prevents backup work during leadership elections, Candidate state, or other transient
/// cluster topology changes where backup responsibility may be undetermined.
/// </summary>
public class ClusterHealthPolicy : IServerBackupPolicy
{
    public static readonly ClusterHealthPolicy Instance = new();

    private ClusterHealthPolicy()
    {
    }

    public bool CanDoBackup(ServerStore serverStore, DateTime now, string databaseName, out string reason)
    {
        var state = serverStore.CurrentRachisState;
        if (state is RachisState.Leader or RachisState.Follower)
        {
            reason = null;
            return true;
        }

        reason = $"Cannot start any backup(s) because cluster is in '{state}' state.";
        return false;
    }
}
