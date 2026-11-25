using System;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

public class ServerStartupPolicy : IServerBackupPolicy
{
    public static readonly ServerStartupPolicy Instance = new();

    private const string Reason = "The server just started.";

    private ServerStartupPolicy()
    {
    }

    public string Name => "Startup";

    public DateTime? HeldUntil => null;

    public bool CanDoBackup(ServerStore serverStore, DateTime now, string databaseName, out string reason, out BackupDecisionCategory category)
    {
        category = BackupDecisionCategory.Waiting;

        if (now - serverStore.Server.Statistics.StartUpTime < serverStore.Configuration.Backup.ServerStartupGracePeriod.AsTimeSpan)
        {
            reason = Reason;
            return false;
        }

        reason = null;
        return true;
    }
}
