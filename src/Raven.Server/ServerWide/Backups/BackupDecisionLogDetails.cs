using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Backups;

public enum BackupDecisionSource
{
    Server,
    Task
}

public enum BackupDecisionKind
{
    Info,
    Policy,
    Started,
    Completed,
    Failed,
    Cancelled
}

public readonly record struct BackupDecision(DateTime Time, BackupDecisionKind Kind, string Detail, string Message);

public sealed class BackupDecisionLogDetails : IDynamicJson
{
    public string NodeTag { get; set; }

    public int MaxEntriesPerLog { get; set; }

    public BackupQueueSummary Queue { get; set; }

    public List<string> Databases { get; set; } = new();

    public List<BackupDecisionLogEntry> Entries { get; set; } = new();

    public int TotalResults { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(NodeTag)] = NodeTag,
            [nameof(MaxEntriesPerLog)] = MaxEntriesPerLog,
            [nameof(Queue)] = Queue?.ToJson(),
            [nameof(Databases)] = new DynamicJsonArray(Databases),
            [nameof(Entries)] = new DynamicJsonArray(Entries.Select(x => x.ToJson())),
            [nameof(TotalResults)] = TotalResults
        };
    }
}

public sealed class BackupQueueSummary : IDynamicJson
{
    public int QueueLength { get; set; }

    public int TrackedTasks { get; set; }

    public int TrackedDatabases { get; set; }

    public int StaleTasks { get; set; }

    public int RunningTasks { get; set; }

    public int BlockedTasks { get; set; }

    public int CurrentNumberOfRunningBackups { get; set; }

    public int MaxNumberOfConcurrentBackups { get; set; }

    public double RunnerFrequencyInSec { get; set; }

    public DateTime? NextBackupUtc { get; set; }

    public string NextBackupDatabase { get; set; }

    public string NextBackupTaskName { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(QueueLength)] = QueueLength,
            [nameof(TrackedTasks)] = TrackedTasks,
            [nameof(TrackedDatabases)] = TrackedDatabases,
            [nameof(StaleTasks)] = StaleTasks,
            [nameof(RunningTasks)] = RunningTasks,
            [nameof(BlockedTasks)] = BlockedTasks,
            [nameof(CurrentNumberOfRunningBackups)] = CurrentNumberOfRunningBackups,
            [nameof(MaxNumberOfConcurrentBackups)] = MaxNumberOfConcurrentBackups,
            [nameof(RunnerFrequencyInSec)] = RunnerFrequencyInSec,
            [nameof(NextBackupUtc)] = NextBackupUtc,
            [nameof(NextBackupDatabase)] = NextBackupDatabase,
            [nameof(NextBackupTaskName)] = NextBackupTaskName
        };
    }
}

public sealed class BackupDecisionLogEntry : IDynamicJson
{
    public DateTime Time { get; set; }

    public BackupDecisionSource Source { get; set; }

    public string Database { get; set; }

    public long? TaskId { get; set; }

    public string TaskName { get; set; }

    public BackupDecisionKind Kind { get; set; }

    public string Detail { get; set; }

    public string Reason { get; set; }

    public static BackupDecisionLogEntry ForServer(BackupDecision decision)
    {
        return Create(decision, BackupDecisionSource.Server);
    }

    public static BackupDecisionLogEntry ForTask(BackupDecision decision, string database, long taskId, string taskName)
    {
        var entry = Create(decision, BackupDecisionSource.Task);

        entry.Database = database;
        entry.TaskId = taskId;
        entry.TaskName = taskName;

        return entry;
    }

    private static BackupDecisionLogEntry Create(BackupDecision decision, BackupDecisionSource source)
    {
        return new BackupDecisionLogEntry
        {
            Time = decision.Time,
            Source = source,
            Kind = decision.Kind,
            Detail = decision.Detail,
            Reason = decision.Message
        };
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Time)] = Time,
            [nameof(Source)] = Source,
            [nameof(Database)] = Database,
            [nameof(TaskId)] = TaskId,
            [nameof(TaskName)] = TaskName,
            [nameof(Kind)] = Kind,
            [nameof(Detail)] = Detail,
            [nameof(Reason)] = Reason
        };
    }
}
