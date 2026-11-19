using System;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

public class DatabaseExistsPolicy : IDatabaseBackupPolicy
{
    public static readonly DatabaseExistsPolicy Instance = new();

    private DatabaseExistsPolicy()
    {
    }

    public string Name => "DatabaseExists";

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, DatabaseBackupState backupState, DateTime now, out string reason, out BackupDecisionCategory category)
    {
        category = BackupDecisionCategory.HeldBack;

        if (serverStore.Cluster.DatabaseExists(context, backupState.DatabaseName) == false)
        {
            reason = "The database doesn't exist.";
            return false;
        }

        reason = null;
        return true;
    }
}
