using System;
using Raven.Client.Documents.Operations.Backups;
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
        return IsDisabled(backupState.Configuration, out reason, out category) == false;
    }

    internal static bool IsDisabled(PeriodicBackupConfiguration configuration, out string reason, out BackupDecisionCategory category)
    {
        category = BackupDecisionCategory.Waiting;

        if (configuration.Disabled)
        {
            reason = "The task is disabled.";
            return true;
        }

        if (configuration.HasBackup() == false)
        {
            reason = "All destinations of the task are disabled.";
            category = BackupDecisionCategory.HeldBack;
            return true;
        }

        reason = null;
        return false;
    }
}
