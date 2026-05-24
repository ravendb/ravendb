using System;
using JetBrains.Annotations;
using Raven.Server.Documents.PeriodicBackup;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

/// <summary>
/// Enforces the server-wide concurrent backup limit tracked by <see cref="ConcurrentBackupsCounter"/>.
/// When the limit is reached, no new backup for the named database is started this cycle; the
/// queue re-evaluates the task on the next tick. Set <see cref="Disabled"/> to true in tests
/// to allow unlimited concurrent backups.
/// </summary>
public class ServerConcurrentBackupPolicy : IServerBackupPolicy
{
    public static bool Disabled;

    private readonly ConcurrentBackupsCounter _concurrentBackupsCounter;

    public ServerConcurrentBackupPolicy([NotNull] ConcurrentBackupsCounter concurrentBackupsCounter)
    {
        _concurrentBackupsCounter = concurrentBackupsCounter ?? throw new ArgumentNullException(nameof(concurrentBackupsCounter));
    }

    public bool CanDoBackup(ServerStore serverStore, DateTime now, string databaseName, out string reason)
    {
        if (Disabled)
        {
            reason = null;
            return true;
        }

        if (_concurrentBackupsCounter.CanRunBackup(databaseName) == false)
        {
            reason = $"Cannot start backup(s) because the maximum number of concurrent backups ({_concurrentBackupsCounter.MaxNumberOfConcurrentBackups}) is reached.";
            return false;
        }

        reason = null;
        return true;
    }
}
