using System;
using Raven.Server.ServerWide.Context;
using static Raven.Server.ServerWide.Backups.ServerBackupRunner;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

/// <summary>
/// Gate evaluated by <see cref="ServerBackupRunner"/> for each (database, backup task) pair during a
/// polling cycle, after all server-level policies have passed. A database backup is launched only when
/// every database policy returns true. Implementations check per-task conditions such as whether a
/// backup is already running, whether the task is due, and whether this node is responsible.
/// </summary>
public interface IDatabaseBackupPolicy
{
    /// <summary>
    /// Returns true when the per-database condition allows this specific backup to proceed.
    /// Sets <paramref name="reason"/> to a human-readable explanation when returning false;
    /// a null reason signals that the database should be woken from idle before retrying.
    /// </summary>
    bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, DatabaseBackupState backupState, DateTime now, out string reason);
}
