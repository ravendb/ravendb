using System;
using Sparrow.LowMemory;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

public class ServerLowMemoryPolicy : IServerBackupPolicy
{
    private const string Reason = "The server is low on memory.";

    private DateTime _heldUntil;

    public string Name => "LowMemory";

    internal DateTime HeldUntil => _heldUntil;

    public bool CanDoBackup(ServerStore serverStore, DateTime now, string databaseName, out string reason, out BackupDecisionCategory category)
    {
        category = BackupDecisionCategory.HeldBack;

        if (serverStore.ForTestingPurposes?.IgnoreHealthChecksBeforeBackup == true)
        {
            reason = null;
            return true;
        }

        if (LowMemoryNotification.Instance.LowMemoryState && now >= _heldUntil)
            _heldUntil = now + serverStore.Configuration.Backup.LowMemoryBackupDelay.AsTimeSpan;

        if (now < _heldUntil)
        {
            reason = $"{Reason} Backups are held until '{_heldUntil}' UTC and then checked again.";
            return false;
        }

        reason = null;
        return true;
    }
}
