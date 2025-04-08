using System;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

public class BackupRunningPolicy : IDatabaseBackupPolicy
{
    public static readonly BackupRunningPolicy Instance = new();

    private BackupRunningPolicy()
    {
    }

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, ServerBackupRunner.DatabaseBackupState backupState, DateTime now, out string reason)
    {
        reason = null;
        return backupState.Running == false;
    }
}
