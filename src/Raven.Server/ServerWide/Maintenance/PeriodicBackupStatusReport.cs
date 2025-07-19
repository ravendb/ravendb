using System;
using Raven.Client.Documents.Operations.Backups;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Maintenance;

public class PeriodicBackupStatusReport : IDynamicJson
{
    public long TaskId { get; set; }
    public BackupType BackupType { get; set; }
    public DateTime? LastSuccessfulFullBackupTime { get; set; }
    public long? LastRaftIndexEtag { get; set; }
    public long? LastFullBackupRaftIndexEtag { get; set; }
    public DateTime? DelayUntil { get; set; }
    public bool IsErrored { get; set; }

    public static PeriodicBackupStatusReport Deserialize(BlittableJsonReaderObject backupStatus)
    {
        if (backupStatus == null)
            return null;

        var statusReport = new PeriodicBackupStatusReport();

        if (backupStatus.TryGet(nameof(PeriodicBackupStatus.TaskId), out long taskId))
            statusReport.TaskId = taskId;

        if (backupStatus.TryGet(nameof(PeriodicBackupStatus.BackupType), out BackupType backupType))
            statusReport.BackupType = backupType;

        if (backupStatus.TryGet(nameof(PeriodicBackupStatus.LastFullBackup), out DateTime? lastSuccessfulFullBackup))
            statusReport.LastSuccessfulFullBackupTime = lastSuccessfulFullBackup;

        if (backupStatus.TryGet(nameof(PeriodicBackupStatus.LastRaftIndex), out BlittableJsonReaderObject lastRaftIndexBlittable) && lastRaftIndexBlittable != null)
        {
            lastRaftIndexBlittable.TryGet(nameof(LastRaftIndex.LastEtag), out long? lastEtag);
            statusReport.LastRaftIndexEtag = lastEtag;

            lastRaftIndexBlittable.TryGet(nameof(LastRaftIndex.LastFullBackupEtag), out long? lastFullBackupEtag);
            statusReport.LastFullBackupRaftIndexEtag = lastFullBackupEtag;
        }

        if (backupStatus.TryGet(nameof(PeriodicBackupStatus.DelayUntil), out DateTime? delayUntil))
            statusReport.DelayUntil = delayUntil;

        if (backupStatus.TryGet(nameof(PeriodicBackupStatus.Error), out BlittableJsonReaderObject errorBlittable) && errorBlittable != null)
            statusReport.IsErrored = true;

        return statusReport;
    }

    public DynamicJsonValue ToJson() =>
        new()
        {
            [nameof(TaskId)] = TaskId,
            [nameof(BackupType)] = BackupType,
            [nameof(LastSuccessfulFullBackupTime)] = LastSuccessfulFullBackupTime,
            [nameof(LastRaftIndexEtag)] = LastRaftIndexEtag,
            [nameof(LastFullBackupRaftIndexEtag)] = LastFullBackupRaftIndexEtag,
            [nameof(DelayUntil)] = DelayUntil,
            [nameof(IsErrored)] = IsErrored
        };

    public override string ToString()
    {
        using var ctx = JsonOperationContext.ShortTermSingleUse();
        return ctx.ReadObject(ToJson(), "backup-status").ToString();
    }
}
