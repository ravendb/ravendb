using System;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

public class BackupRunningPolicy : IDatabaseBackupPolicy
{
    public static readonly BackupRunningPolicy Instance = new();

    private BackupRunningPolicy()
    {
    }

    public string Name => "BackupRunning";

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, DatabaseBackupState backupState, DateTime now, out string reason)
    {
        var running = backupState.Running;
        reason = running ? "Cannot start another backup while a backup is already running." : null;
        return running == false;
    }
}
