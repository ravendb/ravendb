using System;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

/// <summary>
/// Blocks backups for tasks whose responsible node is a different member of the cluster.
/// Preserves the single-writer guarantee: only the node designated by the cluster observer
/// runs a given backup task, preventing duplicate backups across nodes.
/// </summary>
public class BackupShouldRunOnThisNodePolicy : IDatabaseBackupPolicy
{
    public static readonly BackupShouldRunOnThisNodePolicy Instance = new();

    private BackupShouldRunOnThisNodePolicy()
    {
    }

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, DatabaseBackupState backupState, DateTime now, out string reason)
    {
        var nodeTag = BackupUtils.GetResponsibleNodeTag(context, backupState.DatabaseName, backupState.Configuration.TaskId);
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
