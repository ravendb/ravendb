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

    public string Name => "ShouldRunOnThisNode";

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, DatabaseBackupState backupState, DateTime now, out string reason, out BackupDecisionCategory category)
    {
        category = BackupDecisionCategory.Waiting;

        var nodeTag = BackupUtils.GetResponsibleNodeTag(context, backupState.DatabaseName, backupState.Configuration.TaskId);
        if (nodeTag == null)
        {
            reason = "No node is responsible for the task.";
            category = BackupDecisionCategory.HeldBack;
            return false;
        }

        if (nodeTag != serverStore.NodeTag)
        {
            reason = $"Node '{nodeTag}' is responsible for the task.";
            return false;
        }

        reason = null;
        return true;
    }
}
