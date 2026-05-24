using System;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

/// <summary>
/// Blocks backups that are not yet due based on their full or incremental schedule.
/// On the first evaluation for a task, reads the persisted status from the cluster store to
/// initialize <see cref="DatabaseBackupState.NextBackup"/>; subsequent ticks use the cached value.
/// </summary>
public class BackupTimePolicy : IDatabaseBackupPolicy
{
    public static readonly BackupTimePolicy Instance = new();

    private BackupTimePolicy()
    {

    }

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, DatabaseBackupState backupState, DateTime now, out string reason)
    {
        if (backupState.NextBackup == null)
        {
            var backupStatus = BackupUtils.GetBackupStatusFromCluster(context, backupState.DatabaseName, backupState.Configuration.TaskId);
            backupState.NextBackup = BackupUtils.GetNextBackupDetails(new BackupUtils.NextBackupDetailsParameters
            {
                BackupStatus = backupStatus,
                Configuration = backupState.Configuration,
                NodeTag = serverStore.NodeTag
            });
        }

        if (backupState.NextBackup == null)
        {
            reason = $"[POLICY:BackupTime] Cannot start backup {backupState} because next backup time could not be calculated.";
            return false;
        }

        if (backupState.NextBackup.DateTime > now)
        {
            reason = $"[POLICY:BackupTime] Cannot start backup {backupState} because it is not yet time to do so. Next backup will occur at '{backupState.NextBackup.DateTime}'.";
            return false;
        }

        reason = null;
        return true;
    }
}
