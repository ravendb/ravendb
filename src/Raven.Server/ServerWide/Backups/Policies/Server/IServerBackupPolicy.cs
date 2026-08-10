using System;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

public interface IServerBackupPolicy
{
    string Name { get; }

    bool CanDoBackup(ServerStore serverStore, DateTime now, string databaseName, out string reason);
}
