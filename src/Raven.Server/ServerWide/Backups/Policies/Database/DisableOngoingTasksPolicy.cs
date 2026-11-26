using System;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

public sealed class DisableOngoingTasksPolicy : IDatabaseBackupPolicy
{
    public static readonly DisableOngoingTasksPolicy Instance = new();

    private DisableOngoingTasksPolicy()
    {
    }

    public string Name => "DisableOngoingTasks";

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, DatabaseBackupState backupState, DateTime now, out string reason, out BackupDecisionCategory category)
    {
        category = BackupDecisionCategory.HeldBack;

        if (serverStore.DatabasesLandlord.TryGetDatabaseIfLoaded(backupState.DatabaseName, out var database) && database.DisableOngoingTasks)
        {
            reason = "The ongoing tasks of the database are disabled by the 'disable.tasks.marker' file.";
            return false;
        }

        reason = null;
        return true;
    }
}
