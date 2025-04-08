using System;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

public class BackupTimePolicy : IDatabaseBackupPolicy
{
    public static readonly BackupTimePolicy Instance = new();

    private BackupTimePolicy()
    {

    }

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, ServerBackupRunner.DatabaseBackupState backupState, DateTime now, out string reason)
    {
        if (backupState.NextBackup == null)
        {
            var backupStatus = BackupUtils.GetBackupStatusFromCluster(serverStore, context, backupState.DatabaseName, backupState.Configuration.TaskId);
            backupState.NextBackup = BackupUtils.GetNextBackupDetails(new BackupUtils.NextBackupDetailsParameters
            {
                BackupStatus = backupStatus,
                Configuration = backupState.Configuration,
                NodeTag = serverStore.NodeTag,
                ResponsibleNodeTag = serverStore.NodeTag
            });
        }

        if (backupState.NextBackup == null)
        {
            reason = $"Cannot start backup {backupState} because next backup time could not be calculated.";
            return false;
        }

        if (backupState.NextBackup.DateTime > now)
        {
            reason = $"Cannot start backup {backupState} because it is not yet time to do so. Next backup will occur at '{backupState.NextBackup.DateTime}'.";
            return false;
        }

        reason = null;
        return true;
    }
}
