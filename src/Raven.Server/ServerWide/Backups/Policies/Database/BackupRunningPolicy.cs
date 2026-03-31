using System;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

public class BackupRunningPolicy : IDatabaseBackupPolicy
{
    public static readonly BackupRunningPolicy Instance = new();

    private BackupRunningPolicy()
    {
    }

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, DatabaseBackupState backupState, DateTime now, out string reason)
    {
        reason = null;
        if (backupState.Running)
            reason = "Cannot start another backup while a backup is already running.";

        return backupState.Running == false;
    }
}
