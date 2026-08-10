using System;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

public class DatabaseLoadedPolicy : IDatabaseBackupPolicy
{
    public static readonly DatabaseLoadedPolicy Instance = new();

    private DatabaseLoadedPolicy()
    {
    }

    public string Name => "DatabaseLoaded";

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, DatabaseBackupState backupState, DateTime now, out string reason)
    {
        if (serverStore.DatabasesLandlord.TryGetDatabaseIfLoaded(backupState.DatabaseName, out var database) == false)
        {
            reason = null; // we want to wake-up the database
            return true;
        }

        var gracePeriod = serverStore.Configuration.Backup.DatabaseLoadedGracePeriod.AsTimeSpan;
        if (now - database.StartTime < gracePeriod)
        {
            reason = $"Cannot start backup {backupState} because the database was loaded less than {gracePeriod.TotalSeconds:F0} seconds ago.";
            return false;
        }

        reason = null;
        return true;
    }
}
