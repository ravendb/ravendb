using System;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

public class NoChangesPolicy : IDatabaseBackupPolicy
{
    private readonly ServerBackupRunner _serverBackupRunner;

    public NoChangesPolicy([NotNull] ServerBackupRunner serverBackupRunner)
    {
        _serverBackupRunner = serverBackupRunner ?? throw new ArgumentNullException(nameof(serverBackupRunner));
    }

    public string Name => "NoChanges";

    public bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, DatabaseBackupState backupState, DateTime now, out string reason, out BackupDecisionCategory category)
    {
        category = BackupDecisionCategory.Waiting;

        if (_serverBackupRunner.CanSkipIncrementalBackup(context, backupState) == false)
        {
            reason = null;
            return true;
        }

        reason = "The database is idle and nothing changed since the last backup, so the incremental backup is skipped without loading the database.";
        return false;
    }
}
