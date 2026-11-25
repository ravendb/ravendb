using System;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

public abstract class ServerHealthBackupPolicy : IServerBackupPolicy
{
    private DateTime _heldUntil;

    public abstract string Name { get; }

    internal DateTime HeldUntil => _heldUntil;

    protected abstract string HoldReason { get; }

    internal abstract string StartFailureReason { get; }

    protected abstract bool IsTriggered(ServerStore serverStore);

    protected abstract TimeSpan GetHoldDuration(ServerStore serverStore);

    internal bool IsServerUnhealthy(ServerStore serverStore)
    {
        return serverStore.ForTestingPurposes?.IgnoreHealthChecksBeforeBackup != true && IsTriggered(serverStore);
    }

    public bool CanDoBackup(ServerStore serverStore, DateTime now, string databaseName, out string reason, out BackupDecisionCategory category)
    {
        category = BackupDecisionCategory.HeldBack;

        if (serverStore.ForTestingPurposes?.IgnoreHealthChecksBeforeBackup == true)
        {
            reason = null;
            return true;
        }

        if (IsTriggered(serverStore) && now >= _heldUntil)
            _heldUntil = now + GetHoldDuration(serverStore);

        if (now < _heldUntil)
        {
            reason = $"{HoldReason} Backups are held until '{_heldUntil}' UTC and then checked again.";
            return false;
        }

        reason = null;
        return true;
    }
}
