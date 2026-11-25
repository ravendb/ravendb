using System;
using Sparrow.LowMemory;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

public class ServerHighDirtyMemoryPolicy : ServerHealthBackupPolicy
{
    public override string Name => "HighDirtyMemory";

    protected override string HoldReason => "The server has high dirty memory.";

    internal override string StartFailureReason => "the server is in high dirty memory state";

    protected override bool IsTriggered(ServerStore serverStore)
    {
        return LowMemoryNotification.Instance.DirtyMemoryState.IsHighDirty;
    }

    protected override TimeSpan GetHoldDuration(ServerStore serverStore)
    {
        return serverStore.Configuration.Backup.LowMemoryBackupDelay.AsTimeSpan;
    }
}
