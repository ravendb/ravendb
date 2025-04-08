using System;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

public class BackupDisabledPolicy : IDatabaseBackupPolicy
{
    public static readonly BackupDisabledPolicy Instance = new();

    private BackupDisabledPolicy()
    {
    }

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, ServerBackupRunner.DatabaseBackupState backupState, DateTime now, out string reason)
    {
        if (backupState.Configuration.Disabled)
        {
            reason = $"Cannot start backup {backupState} because it is disabled.";
            return false;
        }

        if (backupState.Configuration.HasBackup() == false)
        {
            reason = $"Cannot start backup {backupState} because all destinations are disabled.";
            return false;
        }

        reason = null;
        return true;
    }
}
