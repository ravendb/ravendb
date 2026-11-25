using System;
using Sparrow.LowMemory;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

public class ServerLowMemoryPolicy : ServerHealthBackupPolicy
{
    public override string Name => "LowMemory";

    protected override string HoldReason => "The server is low on memory.";

    internal override string StartFailureReason => "the server is in low memory state";

    protected override bool IsTriggered(ServerStore serverStore)
    {
        return LowMemoryNotification.Instance.LowMemoryState;
    }

    protected override TimeSpan GetHoldDuration(ServerStore serverStore)
    {
        return serverStore.Configuration.Backup.LowMemoryBackupDelay.AsTimeSpan;
    }
}
