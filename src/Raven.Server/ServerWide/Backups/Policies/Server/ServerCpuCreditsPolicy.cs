using System;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

/// <summary>
/// Blocks all backups when the server's CPU credit balance is near exhaustion.
/// Prevents backup work from starving foreground request processing on cloud instances
/// with burstable CPU. Set <see cref="Disabled"/> to true in tests to bypass the check.
/// </summary>
public class ServerCpuCreditsPolicy : IServerBackupPolicy
{
    public static readonly ServerCpuCreditsPolicy Instance = new();

    public static bool Disabled;

    private const string Reason = "Cannot start any backup(s) because server CPU Credits are near exhaustion.";

    private ServerCpuCreditsPolicy()
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
        if (serverStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised())
        {
            reason = Reason;
            return false;
        }

        return true;
    }
}
