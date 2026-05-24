using System;
using Sparrow.LowMemory;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

/// <summary>
/// Blocks all backups when the server's dirty-memory level is high.
/// High dirty memory means the process is holding more unwritten pages than the OS can
/// comfortably flush; adding snapshot I/O at this point risks further memory pressure.
/// Set <see cref="Disabled"/> to true in tests to bypass the check.
/// </summary>
public class ServerHighDirtyMemoryPolicy : IServerBackupPolicy
{
    public static readonly ServerHighDirtyMemoryPolicy Instance = new();

    public static bool Disabled;

    private const string Reason = "[POLICY:HighDirtyMemory] Cannot start any backup(s) because server is in high dirty memory state.";

    private ServerHighDirtyMemoryPolicy()
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
        if (LowMemoryNotification.Instance.DirtyMemoryState.IsHighDirty)
        {
            reason = Reason;
            return false;
        }

        return true;
    }
}
