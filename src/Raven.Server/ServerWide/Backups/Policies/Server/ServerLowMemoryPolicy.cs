using System;
using Sparrow.LowMemory;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

/// <summary>
/// Blocks all backups when the server reports a low-memory condition.
/// Avoids adding I/O and working-set pressure from backup work when the process is already
/// memory-constrained. Set <see cref="Disabled"/> to true in tests to bypass the check.
/// </summary>
public class ServerLowMemoryPolicy : IServerBackupPolicy
{
    public static readonly ServerLowMemoryPolicy Instance = new();

    public static bool Disabled;

    private const string Reason = "Cannot start any backup(s) because server is in low memory state.";

    private ServerLowMemoryPolicy()
    {
    }

    public bool CanDoBackup(ServerStore serverStore, DateTime now, string databaseName, out string reason)
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
