using System;

namespace Raven.Server.ServerWide.Backups.Policies.Server;

public class ServerCpuCreditsPolicy : IServerBackupPolicy
{
    private const string Reason = "The server CPU credits are near exhaustion.";

    private DateTime _heldUntil;

    public string Name => "CpuCredits";

    internal DateTime HeldUntil => _heldUntil;

    public bool CanDoBackup(ServerStore serverStore, DateTime now, string databaseName, out string reason, out BackupDecisionCategory category)
    {
        category = BackupDecisionCategory.HeldBack;

        if (serverStore.ForTestingPurposes?.IgnoreHealthChecksBeforeBackup == true)
        {
            reason = null;
            return true;
        }

        if (serverStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised() && now >= _heldUntil)
            _heldUntil = now + serverStore.Configuration.Server.CpuCreditsExhaustionBackupDelay.AsTimeSpan;

        if (now < _heldUntil)
        {
            reason = $"{Reason} Backups are held until '{_heldUntil}' UTC and then checked again.";
            return false;
        }

        reason = null;
        return true;
    }
}
