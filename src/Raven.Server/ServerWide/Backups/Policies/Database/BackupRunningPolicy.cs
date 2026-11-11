using System;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

public class BackupRunningPolicy : IDatabaseBackupPolicy
{
    public static readonly BackupRunningPolicy Instance = new();

    private BackupRunningPolicy()
    {
    }

    public string Name => "BackupRunning";

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, DatabaseBackupState backupState, DateTime now, out string reason, out BackupDecisionCategory category)
    {
        category = BackupDecisionCategory.Waiting;

        if (backupState.Running)
        {
            reason = "A backup of this task is already running.";
            return false;
        }

        if (backupState.StatusUpdateInProgress)
        {
            reason = "The status of a skipped incremental backup of this task is being saved.";
            return false;
        }

        reason = null;
        return true;
    }
}
