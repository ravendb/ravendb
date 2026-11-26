using System;
using Raven.Server.ServerWide.Context;

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
        var taskStatus = serverStore.ServerBackupRunner.GetTaskStatus(backupState.Configuration, backupState.DatabaseName, out var nodeTag, context);
        return CanRunOnThisNode(taskStatus, nodeTag, backupState, out reason, out category);
    }

    internal static bool CanRunOnThisNode(ServerBackupRunner.TaskStatus taskStatus, string responsibleNodeTag, DatabaseBackupState backupState, out string reason, out BackupDecisionCategory category)
    {
        category = BackupDecisionCategory.Waiting;

        switch (taskStatus)
        {
            case ServerBackupRunner.TaskStatus.Disabled:
                BackupDisabledPolicy.IsDisabled(backupState.Configuration, out reason, out category);
                return false;

            case ServerBackupRunner.TaskStatus.MissingResponsibleNode:
                reason = "No node is responsible for the task.";
                category = BackupDecisionCategory.HeldBack;
                return false;

            case ServerBackupRunner.TaskStatus.ActiveByOtherNode:
                reason = $"Node '{responsibleNodeTag}' is responsible for the task.";
                return false;

            case ServerBackupRunner.TaskStatus.ActiveByCurrentNode:
                reason = null;
                return true;

            default:
                throw new ArgumentOutOfRangeException(nameof(taskStatus), taskStatus, $"Unknown backup task status '{taskStatus}' of {backupState}");
        }
    }
}
