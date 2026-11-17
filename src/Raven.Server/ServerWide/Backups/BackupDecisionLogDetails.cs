using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Exceptions;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Backups;

public enum BackupDecisionSource
{
    Server,
    Task
}

public enum BackupDecisionCategory
{
    Waiting,
    HeldBack,
    Started,
    Completed,
    Failed,
    Cancelled
}

public sealed class BackupDecision
{
    public long Id { get; init; }

    public DateTime FirstTime { get; init; }

    public DateTime LastTime { get; internal set; }

    public int Count { get; internal set; }

    public BackupDecisionCategory Category { get; init; }

    public string Detail { get; init; }

    public string Reason { get; init; }

    internal long Etag { get; set; }

    internal bool IsSameAs(BackupDecisionCategory category, string detail, string reason)
    {
        return Category == category &&
               string.Equals(Detail, detail, StringComparison.Ordinal) &&
               string.Equals(Reason, reason, StringComparison.Ordinal);
    }
}

public sealed class BackupDecisionLog
{
    private readonly BackupDecision[] _decisions;

    private int _newestIndex = -1;

    private int _count;

    public BackupDecisionLog(int maxSize)
    {
        _decisions = new BackupDecision[maxSize];
    }

    internal BackupDecision Add(BackupDecisionCategory category, string detail, string reason, DateTime time, Func<long> nextId, Func<long> nextEtag)
    {
        lock (_decisions)
        {
            if (_count > 0 && _decisions[_newestIndex].IsSameAs(category, detail, reason))
            {
                var latest = _decisions[_newestIndex];
                latest.Count++;
                latest.LastTime = time;
                latest.Etag = nextEtag();

                return latest;
            }

            var decision = new BackupDecision
            {
                Id = nextId(),
                FirstTime = time,
                LastTime = time,
                Count = 1,
                Category = category,
                Detail = detail,
                Reason = reason,
                Etag = nextEtag()
            };

            _newestIndex = (_newestIndex + 1) % _decisions.Length;
            _decisions[_newestIndex] = decision;

            if (_count < _decisions.Length)
                _count++;

            return decision;
        }
    }

    public List<BackupDecision> GetDecisions()
    {
        lock (_decisions)
        {
            var decisions = new List<BackupDecision>(_count);

            for (var i = 0; i < _count; i++)
                decisions.Add(Copy(GetNewest(i)));

            return decisions;
        }
    }

    internal List<BackupDecision> GetMatchingDecisions(long? afterEtag, BackupDecisionLogFilter filter, int take, out int totalMatches)
    {
        var decisions = new List<BackupDecision>();
        totalMatches = 0;

        lock (_decisions)
        {
            for (var i = 0; i < _count; i++)
            {
                var decision = GetNewest(i);
                if (afterEtag != null && decision.Etag <= afterEtag)
                    break;

                if (filter.Matches(decision) == false)
                    continue;

                totalMatches++;

                if (decisions.Count < take)
                    decisions.Add(Copy(decision));
            }
        }

        return decisions;
    }

    public bool TryGetLatest(out BackupDecision decision)
    {
        lock (_decisions)
        {
            if (_count == 0)
            {
                decision = null;
                return false;
            }

            decision = Copy(_decisions[_newestIndex]);
            return true;
        }
    }

    private BackupDecision GetNewest(int index)
    {
        return _decisions[(_newestIndex - index + _decisions.Length) % _decisions.Length];
    }

    private static BackupDecision Copy(BackupDecision decision)
    {
        return new BackupDecision
        {
            Id = decision.Id,
            FirstTime = decision.FirstTime,
            LastTime = decision.LastTime,
            Count = decision.Count,
            Category = decision.Category,
            Detail = decision.Detail,
            Reason = decision.Reason,
            Etag = decision.Etag
        };
    }
}

public sealed class BackupDecisionLogFilter
{
    public HashSet<string> Databases { get; set; }

    public HashSet<long> TaskIds { get; set; }

    public HashSet<BackupDecisionCategory> Categories { get; set; }

    public string Reason { get; set; }

    public DateTime? From { get; set; }

    public DateTime? To { get; set; }

    public int Take { get; set; }

    internal bool MatchesTask(string database, long taskId)
    {
        return (Databases is not { Count: > 0 } || Databases.Contains(database)) &&
               (TaskIds is not { Count: > 0 } || TaskIds.Contains(taskId));
    }

    internal bool Matches(BackupDecision decision)
    {
        if (Categories is { Count: > 0 } && Categories.Contains(decision.Category) == false)
            return false;

        if (string.IsNullOrEmpty(Reason) == false && (decision.Reason == null || decision.Reason.Contains(Reason, StringComparison.OrdinalIgnoreCase) == false))
            return false;

        if (From != null && decision.LastTime < From.Value)
            return false;

        if (To != null && decision.FirstTime > To.Value)
            return false;

        return true;
    }

    internal static long ParseTaskId(string value)
    {
        if (long.TryParse(value, out var taskId) == false)
            throw new BadRequestException($"Invalid task id '{value}'. A task id must be a number.");

        return taskId;
    }

    internal static BackupDecisionCategory ParseCategory(string value)
    {
        if (Enum.TryParse<BackupDecisionCategory>(value, ignoreCase: true, out var category) == false || Enum.IsDefined(category) == false)
            throw new BadRequestException($"Invalid category '{value}'. Valid categories are: {string.Join(", ", Enum.GetNames<BackupDecisionCategory>())}.");

        return category;
    }
}

public sealed class BackupDecisionLogDetails : IDynamicJson
{
    public string NodeTag { get; set; }

    public DateTime RunnerStartTime { get; set; }

    public BackupDecisionLogSummary Summary { get; set; }

    public List<BackupDecisionLogEntry> Entries { get; set; } = new();

    public int TotalResults { get; set; }

    public List<string> Databases { get; set; } = new();

    public List<BackupDecisionLogTask> Tasks { get; set; } = new();

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(NodeTag)] = NodeTag,
            [nameof(RunnerStartTime)] = RunnerStartTime,
            [nameof(Summary)] = Summary?.ToJson(),
            [nameof(Entries)] = new DynamicJsonArray(Entries.Select(x => x.ToJson())),
            [nameof(TotalResults)] = TotalResults,
            [nameof(Databases)] = Databases == null ? null : new DynamicJsonArray(Databases),
            [nameof(Tasks)] = Tasks == null ? null : new DynamicJsonArray(Tasks.Select(x => x.ToJson()))
        };
    }
}

public sealed class BackupDecisionLogTask : IDynamicJson
{
    public string Database { get; set; }

    public long TaskId { get; set; }

    public string TaskName { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Database)] = Database,
            [nameof(TaskId)] = TaskId,
            [nameof(TaskName)] = TaskName
        };
    }
}

public sealed class BackupDecisionLogSummary : IDynamicJson
{
    public RunningSummary Running { get; set; } = new();

    public WaitingForScheduleSummary WaitingForSchedule { get; set; } = new();

    public HeldBackSummary HeldBack { get; set; } = new();

    public ServerBlockSummary ServerBlock { get; set; }

    public OutcomeSummary Failed { get; set; } = new();

    public OutcomeSummary Completed { get; set; } = new();

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Running)] = Running.ToJson(),
            [nameof(WaitingForSchedule)] = WaitingForSchedule.ToJson(),
            [nameof(HeldBack)] = HeldBack.ToJson(),
            [nameof(ServerBlock)] = ServerBlock?.ToJson(),
            [nameof(Failed)] = Failed.ToJson(),
            [nameof(Completed)] = Completed.ToJson()
        };
    }

    public sealed class RunningSummary
    {
        public int Current { get; set; }

        public int Max { get; set; }

        public int WaitingForSlot { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Current)] = Current,
                [nameof(Max)] = Max,
                [nameof(WaitingForSlot)] = WaitingForSlot
            };
        }
    }

    public sealed class WaitingForScheduleSummary
    {
        public int Count { get; set; }

        public DateTime? NextBackupUtc { get; set; }

        public string NextBackupDatabase { get; set; }

        public string NextBackupTaskName { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Count)] = Count,
                [nameof(NextBackupUtc)] = NextBackupUtc,
                [nameof(NextBackupDatabase)] = NextBackupDatabase,
                [nameof(NextBackupTaskName)] = NextBackupTaskName
            };
        }
    }

    public sealed class HeldBackSummary
    {
        public int Count { get; set; }

        public HeldBackTask Longest { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Count)] = Count,
                [nameof(Longest)] = Longest?.ToJson()
            };
        }
    }

    public sealed class HeldBackTask
    {
        public string Database { get; set; }

        public long TaskId { get; set; }

        public string TaskName { get; set; }

        public string Detail { get; set; }

        public string Reason { get; set; }

        public DateTime Since { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Database)] = Database,
                [nameof(TaskId)] = TaskId,
                [nameof(TaskName)] = TaskName,
                [nameof(Detail)] = Detail,
                [nameof(Reason)] = Reason,
                [nameof(Since)] = Since
            };
        }
    }

    public sealed class ServerBlockSummary
    {
        public string Detail { get; set; }

        public string Reason { get; set; }

        public DateTime Since { get; set; }

        public DateTime? Until { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Detail)] = Detail,
                [nameof(Reason)] = Reason,
                [nameof(Since)] = Since,
                [nameof(Until)] = Until
            };
        }
    }

    public sealed class OutcomeSummary
    {
        public long Count { get; set; }

        public Outcome Latest { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Count)] = Count,
                [nameof(Latest)] = Latest?.ToJson()
            };
        }
    }

    public sealed class Outcome
    {
        public string Database { get; set; }

        public long TaskId { get; set; }

        public string TaskName { get; set; }

        public DateTime Time { get; set; }

        public string Error { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Database)] = Database,
                [nameof(TaskId)] = TaskId,
                [nameof(TaskName)] = TaskName,
                [nameof(Time)] = Time,
                [nameof(Error)] = Error
            };
        }
    }
}

public sealed class BackupDecisionLogEntry : IDynamicJson
{
    public long Id { get; set; }

    public DateTime FirstTime { get; set; }

    public DateTime LastTime { get; set; }

    public int Count { get; set; }

    public BackupDecisionCategory Category { get; set; }

    public string Detail { get; set; }

    public string Reason { get; set; }

    public BackupDecisionSource Source { get; set; }

    public string Database { get; set; }

    public int? Shard { get; set; }

    public long? TaskId { get; set; }

    public string TaskName { get; set; }

    public string NodeTag { get; set; }

    internal long Etag { get; set; }

    public static BackupDecisionLogEntry Create(BackupDecision decision, string nodeTag, DatabaseBackupState backupState = null)
    {
        var entry = new BackupDecisionLogEntry
        {
            Id = decision.Id,
            FirstTime = decision.FirstTime,
            LastTime = decision.LastTime,
            Count = decision.Count,
            Category = decision.Category,
            Detail = decision.Detail,
            Reason = decision.Reason,
            Source = backupState == null ? BackupDecisionSource.Server : BackupDecisionSource.Task,
            NodeTag = nodeTag,
            Etag = decision.Etag
        };

        if (backupState == null)
            return entry;

        entry.Database = backupState.OriginalDatabaseName;
        entry.Shard = ShardHelper.TryGetShardNumberFromDatabaseName(backupState.DatabaseName, out var shardNumber) ? shardNumber : null;
        entry.TaskId = backupState.Configuration.TaskId;
        entry.TaskName = backupState.Configuration.Name;

        return entry;
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Id)] = Id,
            [nameof(FirstTime)] = FirstTime,
            [nameof(LastTime)] = LastTime,
            [nameof(Count)] = Count,
            [nameof(Category)] = Category,
            [nameof(Detail)] = Detail,
            [nameof(Reason)] = Reason,
            [nameof(Source)] = Source,
            [nameof(Database)] = Database,
            [nameof(Shard)] = Shard,
            [nameof(TaskId)] = TaskId,
            [nameof(TaskName)] = TaskName,
            [nameof(NodeTag)] = NodeTag
        };
    }
}
