using System;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

public class BackupTimePolicy : IDatabaseBackupPolicy
{
    public static readonly BackupTimePolicy Instance = new();

    private BackupTimePolicy()
    {

    }

    public string Name => "BackupTime";

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, DatabaseBackupState backupState, DateTime now, out string reason, out BackupDecisionCategory category)
    {
        category = BackupDecisionCategory.Waiting;

        if (backupState.NextBackup == null)
        {
            reason = "The next backup time can't be calculated.";
            category = BackupDecisionCategory.HeldBack;
            return false;
        }

        if (backupState.NextBackup.DateTime > now)
        {
            reason = $"The backup isn't due yet. Next backup at '{backupState.NextBackup.DateTime}' UTC.";
            return false;
        }

        reason = null;
        return true;
    }
}
