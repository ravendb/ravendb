using System;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

public class DatabaseExistsPolicy : IDatabaseBackupPolicy
{
    public static readonly DatabaseExistsPolicy Instance = new();

    private DatabaseExistsPolicy()
    {
    }

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, ServerBackupRunner.DatabaseBackupState backupState, DateTime now, out string reason)
    {
        if (serverStore.Cluster.DatabaseExists(backupState.DatabaseName) == false)
        {
            reason = $"Cannot start backup {backupState} because database doesn't exist.";
            return false;
        }

        reason = null;
        return true;
    }
}
