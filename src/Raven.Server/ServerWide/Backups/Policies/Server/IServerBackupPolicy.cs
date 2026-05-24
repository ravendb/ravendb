using System;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

/// <summary>
/// Gate evaluated by <see cref="ServerBackupRunner"/> once per polling cycle, before any per-database
/// policy is checked. If any server policy returns false the entire batch is skipped for that tick.
/// Implementations check server-wide conditions such as memory pressure, CPU credits, cluster health,
/// and startup grace periods that apply equally to every database on the node.
/// </summary>
public interface IServerBackupPolicy
{
    /// <summary>
    /// Returns true when the server condition allows backups to proceed this cycle.
    /// Sets <paramref name="reason"/> to a human-readable explanation when returning false.
    /// </summary>
    bool CanDoBackup(ServerStore serverStore, DateTime now, string databaseName, out string reason);
}
