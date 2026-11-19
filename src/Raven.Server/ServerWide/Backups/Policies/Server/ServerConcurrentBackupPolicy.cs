using System;
using JetBrains.Annotations;
using Raven.Server.Documents.PeriodicBackup;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

public class ServerConcurrentBackupPolicy : IServerBackupPolicy
{
    private readonly ConcurrentBackupsCounter _concurrentBackupsCounter;

    public ServerConcurrentBackupPolicy([NotNull] ConcurrentBackupsCounter concurrentBackupsCounter)
    {
        _concurrentBackupsCounter = concurrentBackupsCounter ?? throw new ArgumentNullException(nameof(concurrentBackupsCounter));
    }

    public string Name => "ConcurrentBackup";

    public bool CanDoBackup(ServerStore serverStore, DateTime now, string databaseName, out string reason, out BackupDecisionCategory category)
    {
        category = BackupDecisionCategory.Waiting;

        if (_concurrentBackupsCounter.CanRunBackup(databaseName) == false)
        {
            reason = $"The maximum number of concurrent backups ({_concurrentBackupsCounter.MaxNumberOfConcurrentBackups}) is reached.";
            return false;
        }

        reason = null;
        return true;
    }
}
