using System;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

public class DatabaseLoadedPolicy : IDatabaseBackupPolicy
{
    public static readonly DatabaseLoadedPolicy Instance = new();

    private DatabaseLoadedPolicy()
    {
    }

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, ServerBackupRunner.DatabaseBackupState backupState, DateTime now, out string reason)
    {
        if (serverStore.DatabasesLandlord.TryGetDatabaseIfLoaded(backupState.DatabaseName, out var database) == false)
        {
            reason = null; // we want to wake-up the database
            return true;
        }

        if (now - database.StartTime < TimeSpan.FromMinutes(1))
        {
            reason = $"Cannot start backup {backupState} because the database was loaded less than 1 minute ago.";
            return false;
        }

        reason = null;
        return true;
    }
}
