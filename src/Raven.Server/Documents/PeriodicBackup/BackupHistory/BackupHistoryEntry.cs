using System;
using Raven.Client.Documents.Operations.Backups;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.PeriodicBackup.BackupHistory;

public class BackupHistoryEntry : IDynamicJson
{
    public BackupHistoryEntry() { }

    public BackupHistoryEntry(PeriodicBackupStatus status, string taskName, BackupTrigger trigger, bool skipped)
    {
        CreatedAt = (status.IsFull ? status.LastFullBackupInternal : status.LastIncrementalBackupInternal)
                    ?? throw new ArgumentException($"The backup status of task {status.TaskId} has no {(status.IsFull ? "full" : "incremental")} backup start time", nameof(status));

        DurationInMs = status.DurationInMs;
        Error = status.Error?.Exception;
        BackupType = status.BackupType;
        BackupKind = status.IsFull ? BackupKind.Full : BackupKind.Incremental;
        NodeTag = status.NodeTag;
        LastFullBackup = status.LastFullBackup;
        TaskName = taskName;
        Trigger = trigger;
        Status = Error != null ? BackupRunStatus.Failed : skipped ? BackupRunStatus.Skipped : BackupRunStatus.Completed;
    }

    public DateTime CreatedAt { get; set; }
    public long? DurationInMs { get; set; }
    public string Error { get; set; }
    public BackupType BackupType { get; set; }
    public BackupKind BackupKind { get; set; }
    public string NodeTag { get; set; }
    public DateTime? LastFullBackup { get; set; }
    public string TaskName { get; set; }
    public BackupTrigger? Trigger { get; set; }
    public BackupRunStatus? Status { get; set; }

    internal long GetFullBackupTicks() => BackupKind == BackupKind.Full ? CreatedAt.Ticks : LastFullBackup?.Ticks ?? 0;

    public DynamicJsonValue ToJson() =>
        new()
        {
            [nameof(CreatedAt)] = CreatedAt,
            [nameof(DurationInMs)] = DurationInMs,
            [nameof(Error)] = Error,
            [nameof(BackupType)] = BackupType,
            [nameof(BackupKind)] = BackupKind,
            [nameof(NodeTag)] = NodeTag,
            [nameof(LastFullBackup)] = LastFullBackup,
            [nameof(TaskName)] = TaskName,
            [nameof(Trigger)] = Trigger,
            [nameof(Status)] = Status,
        };
}
