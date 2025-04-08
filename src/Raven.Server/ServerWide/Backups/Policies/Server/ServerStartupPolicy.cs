using System;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

public class ServerStartupPolicy : IServerBackupPolicy
{
    public static readonly ServerStartupPolicy Instance = new();

    public static bool Disabled = false;

    private const string Reason = "Cannot start any backup(s) because server just started.";

    private ServerStartupPolicy()
    {
    }

    public bool CanDoBackup(ServerStore serverStore, DateTime now, out string reason)
    {
        if (Disabled)
        {
            reason = null;
            return true;
        }

        if (now - serverStore.Server.Statistics.StartUpTime < TimeSpan.FromMinutes(5))
        {
            reason = Reason;
            return false;
        }

        reason = null;
        return true;
    }
}
