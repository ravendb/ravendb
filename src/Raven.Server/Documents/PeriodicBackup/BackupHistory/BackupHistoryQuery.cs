using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.Backups;

namespace Raven.Server.Documents.PeriodicBackup.BackupHistory;

public sealed class BackupHistoryQuery
{
    public int? ShardNumber { get; init; }
    public long? TaskId { get; init; }
    public long? FullBackupTicks { get; init; }
    public bool IncludeIncrementals { get; init; } = true;
    public DateTime? From { get; init; }
    public DateTime? To { get; init; }
    public string TaskName { get; init; }
    public HashSet<BackupTrigger> Triggers { get; init; } = [];
    public HashSet<BackupType> BackupTypes { get; init; } = [];
    public int Start { get; init; }
    public int PageSize { get; init; } = int.MaxValue;

    internal bool MatchesTaskName(string taskName) =>
        string.IsNullOrEmpty(TaskName) || (taskName?.Contains(TaskName, StringComparison.OrdinalIgnoreCase) ?? false);

    internal bool IsInTimeRange(BackupHistoryEntry entry) =>
        (From.HasValue == false || entry.CreatedAt >= From.Value) && (To.HasValue == false || entry.CreatedAt <= To.Value);

    internal bool MatchesValues(BackupHistoryEntry entry) =>
        (Triggers.Count == 0 || (entry.Trigger.HasValue && Triggers.Contains(entry.Trigger.Value))) &&
        (BackupTypes.Count == 0 || BackupTypes.Contains(entry.BackupType));
}
