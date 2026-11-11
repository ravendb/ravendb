using System;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

public class DatabaseStatePolicy : IDatabaseBackupPolicy
{
    public static readonly DatabaseStatePolicy Instance = new();

    private DatabaseStatePolicy()
    {
    }

    public string Name => "DatabaseState";

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, DatabaseBackupState backupState, DateTime now, out string reason, out BackupDecisionCategory category)
    {
        category = BackupDecisionCategory.Waiting;

        using (var rawRecord = serverStore.Cluster.ReadRawDatabaseRecord(context, backupState.OriginalDatabaseName))
        {
            if (rawRecord == null)
            {
                reason = null;
                return true;
            }

            if (rawRecord.IsDisabled)
            {
                reason = "The database is disabled.";
                return false;
            }

            if (rawRecord.DatabaseState == DatabaseStateStatus.RestoreInProgress)
            {
                reason = "The database is being restored.";
                return false;
            }
        }

        reason = null;
        return true;
    }
}
