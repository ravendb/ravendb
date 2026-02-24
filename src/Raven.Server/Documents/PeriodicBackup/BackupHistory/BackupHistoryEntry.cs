using System;
using Raven.Client.Documents.Operations.Backups;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.PeriodicBackup.BackupHistory;

public class BackupHistoryEntry : IDynamicJson
{
    public BackupHistoryEntry() { }

    public BackupHistoryEntry(PeriodicBackupStatus status)
    {
        CreatedAt =
            (status.IsFull ? status.LastFullBackup : status.LastIncrementalBackup)
            ?? status.Error.At;

        DurationInMs = status.DurationInMs;
        Error = status.Error?.Exception;
        BackupType = status.BackupType;
        BackupKind = status.IsFull ? BackupKind.Full : BackupKind.Incremental;
        NodeTag = status.NodeTag;
        LastFullBackup = status.LastFullBackup;
    }

    public DateTime CreatedAt { get; set; }
    public long? DurationInMs { get; set; }
    public string Error { get; set; }
    public BackupType BackupType { get; set; }
    public BackupKind BackupKind { get; set; }
    public string NodeTag { get; set; }
    public DateTime? LastFullBackup { get; set; }

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
        };
}
