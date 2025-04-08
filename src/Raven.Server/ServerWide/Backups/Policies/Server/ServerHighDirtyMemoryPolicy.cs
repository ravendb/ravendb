using System;
using Sparrow.LowMemory;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

public class ServerHighDirtyMemoryPolicy : IServerBackupPolicy
{
    public static readonly ServerHighDirtyMemoryPolicy Instance = new();

    public static bool Disabled;

    private const string Reason = "Cannot start any backup(s) because server is in high dirty memory state.";

    private ServerHighDirtyMemoryPolicy()
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
        if (LowMemoryNotification.Instance.DirtyMemoryState.IsHighDirty)
        {
            reason = Reason;
            return false;
        }

        return true;
    }
}
