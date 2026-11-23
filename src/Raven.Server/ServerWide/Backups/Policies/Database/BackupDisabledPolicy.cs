using System;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

public class BackupDisabledPolicy : IDatabaseBackupPolicy
{
    public static readonly BackupDisabledPolicy Instance = new();

    private BackupDisabledPolicy()
    {
    }

    public string Name => "BackupDisabled";

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, DatabaseBackupState backupState, DateTime now, out string reason, out BackupDecisionCategory category)
    {
        category = BackupDecisionCategory.Waiting;

        if (backupState.Configuration.Disabled)
        {
            reason = "The task is disabled.";
            return false;
        }

        if (backupState.Configuration.HasBackup() == false)
        {
            reason = "All destinations of the task are disabled.";
            category = BackupDecisionCategory.HeldBack;
            return false;
        }

        reason = null;
        return true;
    }
}
