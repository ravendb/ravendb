using System;
using Raven.Server.ServerWide.Context;
using static Raven.Server.ServerWide.Backups.ServerBackupRunner;

namespace Raven.Server.ServerWide.Backups.Policies.Database;

public interface IDatabaseBackupPolicy
{
    bool CanDoBackup(ClusterOperationContext context, ServerStore serverStore, DatabaseBackupState backupState, DateTime now, out string reason);
}
