using System;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

/// <summary>
/// Suppresses backups during the grace period immediately after a database wakes from idle.
/// When the database is not loaded at all, the policy returns true (with a null reason) so
/// that the runner wakes it; once loaded, it enforces a configurable settling window before
/// the first backup is allowed to run.
/// </summary>
public class DatabaseLoadedPolicy : IDatabaseBackupPolicy
{
    public static readonly DatabaseLoadedPolicy Instance = new();

    private DatabaseLoadedPolicy()
    {
    }

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
            reason = $"[POLICY:DatabaseLoaded] Cannot start backup {backupState} because the database was loaded less than {gracePeriod.TotalSeconds:F0} seconds ago.";
            return false;
        }

        reason = null;
        return true;
    }
}
