using System;
using Sparrow.LowMemory;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

public class ServerLowMemoryPolicy : IServerBackupPolicy
{
    public static readonly ServerLowMemoryPolicy Instance = new();

    public static bool Disabled;

    private const string Reason = "Cannot start any backup(s) because server is in low memory state.";

    private ServerLowMemoryPolicy()
    {
    }
    
    public bool CanDoBackup(ServerStore serverStore, DateTime now, out string reason)
    {
        if (Disabled)
        {
            reason = null;
            return true;
        }

        reason = null;
        if (LowMemoryNotification.Instance.LowMemoryState)
        {
            reason = Reason;
            return false;
        }

        return true;
    }
}
