using System;
using JetBrains.Annotations;
using Raven.Server.Documents.PeriodicBackup;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

public class ServerConcurrentBackupPolicy : IServerBackupPolicy
{
    public static bool Disabled;

    private readonly ConcurrentBackupsCounter _concurrentBackupsCounter;

    public ServerConcurrentBackupPolicy([NotNull] ConcurrentBackupsCounter concurrentBackupsCounter)
    {
        _concurrentBackupsCounter = concurrentBackupsCounter ?? throw new ArgumentNullException(nameof(concurrentBackupsCounter));
    }

    public bool CanDoBackup(ServerStore serverStore, DateTime now, out string reason)
    {
        if (Disabled)
        {
            reason = null;
            return true;
        }

        if (_concurrentBackupsCounter.CanRunBackup(string.Empty) == false)
        {
            reason = $"Cannot start backup(s) because the maximum number of concurrent backups ({_concurrentBackupsCounter.MaxNumberOfConcurrentBackups}) is reached.";
            return false;
        }

        reason = null;
        return true;
    }
}
