using System;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

public interface IServerBackupPolicy
{
    bool CanDoBackup(ServerStore serverStore, DateTime now, out string reason);
}
