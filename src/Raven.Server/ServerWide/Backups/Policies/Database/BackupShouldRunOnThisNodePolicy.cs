using System;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

public class BackupShouldRunOnThisNodePolicy : IDatabaseBackupPolicy
{
    public static readonly BackupShouldRunOnThisNodePolicy Instance = new();

    private BackupShouldRunOnThisNodePolicy()
    {
    }

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, ServerBackupRunner.DatabaseBackupState backupState, DateTime now, out string reason)
    {
        var nodeTag = BackupUtils.GetResponsibleNodeTag(context, serverStore, backupState.DatabaseName, backupState.Configuration.TaskId);
        if (nodeTag == null)
        {
            reason = $"Cannot start backup {backupState} because no node is responsible for this task.";
            return false;
        }

        if (nodeTag != serverStore.NodeTag)
        {
            reason = $"Cannot start backup {backupState} because this node is not responsible for this task.";
            return false;
        }

        reason = null;
        return true;
    }
}
