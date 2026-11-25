using System;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

public class ServerCpuCreditsPolicy : ServerHealthBackupPolicy
{
    public override string Name => "CpuCredits";

    protected override string HoldReason => "The server CPU credits are near exhaustion.";

    internal override string StartFailureReason => "the CPU credits allocated to this machine are nearing exhaustion";

    protected override bool IsTriggered(ServerStore serverStore)
    {
        return serverStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised();
    }

    protected override TimeSpan GetHoldDuration(ServerStore serverStore)
    {
        return serverStore.Configuration.Server.CpuCreditsExhaustionBackupDelay.AsTimeSpan;
    }
}
