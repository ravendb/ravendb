using System;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

public class ServerCpuCreditsPolicy : IServerBackupPolicy
{
    public static readonly ServerCpuCreditsPolicy Instance = new();

    public static bool Disabled;

    private const string Reason = "Cannot start any backup(s) because server CPU Creduts are near exhaustion.";

    private ServerCpuCreditsPolicy()
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
        if (serverStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised())
        {
            reason = Reason;
            return false;
        }

        return true;
    }
}
