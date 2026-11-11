using System;
using Raven.Client.ServerWide;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

public class ClusterHealthPolicy : IServerBackupPolicy
{
    public static readonly ClusterHealthPolicy Instance = new();

    private ClusterHealthPolicy()
    {
    }

    public string Name => "ClusterHealth";

    public bool CanDoBackup(ServerStore serverStore, DateTime now, string databaseName, out string reason, out BackupDecisionCategory category)
    {
        category = BackupDecisionCategory.HeldBack;

        var state = serverStore.CurrentRachisState;
        if (state is RachisState.Leader or RachisState.Follower)
        {
            reason = null;
            return true;
        }

        reason = $"The cluster is in '{state}' state.";
        return false;
    }
}
