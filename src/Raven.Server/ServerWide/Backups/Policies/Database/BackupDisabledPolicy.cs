using System;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

/// <summary>
/// Blocks backups for a task that has been explicitly disabled or has all backup destinations turned off.
/// A task with no active destinations is treated as disabled even if the task itself is not flagged as such.
/// </summary>
public class BackupDisabledPolicy : IDatabaseBackupPolicy
{
    public static readonly BackupDisabledPolicy Instance = new();

    private BackupDisabledPolicy()
    {
    }

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, DatabaseBackupState backupState, DateTime now, out string reason)
    {
        if (backupState.Configuration.Disabled)
        {
            reason = $"[POLICY:BackupDisabled] Cannot start backup {backupState} because it is disabled.";
            return false;
        }

        if (backupState.Configuration.HasBackup() == false)
        {
            reason = $"[POLICY:BackupDisabled] Cannot start backup {backupState} because all destinations are disabled.";
            return false;
        }

        reason = null;
        return true;
    }
}
