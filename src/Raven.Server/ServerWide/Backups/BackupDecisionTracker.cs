using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Backups.Policies.Server;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Sparrow.Server.Utils;

namespace Raven.Server.ServerWide.Backups;

public sealed class BackupDecisionTracker
{
    public const int MaxDecisionLogSize = 1024;

    internal const string CancelReasonDisabled = "disabled";
    internal const string CancelReasonTaskDeleted = "task-deleted";
    internal const string CancelReasonDelay = "delay";
    internal const string CancelReasonUnloaded = "unloaded";
    internal const string CancelReasonUser = "user";

    internal const string DisableOngoingTasksDetail = "DisableOngoingTasks";

    private readonly ServerBackupRunner _runner;

    private readonly ServerStore _serverStore;

    private readonly RavenLogger _logger;

    private readonly BackupDecisionLog _serverDecisionLog = new(MaxDecisionLogSize);

    private readonly object _decisionEtagLock = new();

    private long _lastDecisionId;

    private long _lastDecisionEtag;

    private TaskCompletionSource<object> _changed = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private BackupDecisionLogSummary _scheduleSummary;

    private long _summaryVersion;

    private long _failedCount;

    private long _completedCount;

    private BackupDecisionLogSummary.Outcome _latestFailure;

    private BackupDecisionLogSummary.Outcome _latestCompletion;

    private DateTime _runnerStartTime;

    private DateTime? _serverHeldUntil;

    internal BackupDecisionTracker(ServerBackupRunner runner, ServerStore serverStore, RavenLogger logger)
    {
        _runner = runner;
        _serverStore = serverStore;
        _logger = logger;
    }

    internal void Start()
    {
        _runnerStartTime = DateTime.UtcNow;
        _serverStore.ConcurrentBackupsCounter.RunningBackupsChanged += _ => OnRunningBackupsChanged();
    }

    internal static string GetCancelReasonText(string cancelReason)
    {
        return cancelReason switch
        {
            CancelReasonDisabled => "The task was disabled while its backup was running.",
            CancelReasonTaskDeleted => "The task or its database was deleted, or the database was removed from this node, while its backup was starting or running.",
            CancelReasonDelay => "The backup was delayed.",
            CancelReasonUnloaded => "The database or the server shut down while the backup was running.",
            _ => "The backup was stopped by a user or through the API."
        };
    }

    internal BackupDecision AddServerDecision(BackupDecisionCategory category, string detail, string reason, DateTime now, DateTime? heldUntil)
    {
        lock (_decisionEtagLock)
        {
            _serverHeldUntil = heldUntil;
            return _serverDecisionLog.Add(category, detail, reason, now, NextDecisionId, NextDecisionEtag);
        }
    }

    internal void AddDecision(DatabaseBackupState backupState, BackupDecisionLog decisionLog, BackupDecisionCategory category, string detail, string reason, DateTime now)
    {
        lock (_decisionEtagLock)
        {
            var decision = decisionLog.Add(category, detail, reason, now, NextDecisionId, NextDecisionEtag);

            if (category is BackupDecisionCategory.Completed or BackupDecisionCategory.Failed)
            {
                OnBackupOutcome(backupState, decision);
                _summaryVersion++;
                TaskExecutor.CompleteAndReplace(ref _changed);
            }
        }
    }

    private long NextDecisionId()
    {
        return ++_lastDecisionId;
    }

    private long NextDecisionEtag()
    {
        _lastDecisionEtag++;
        TaskExecutor.CompleteAndReplace(ref _changed);

        return _lastDecisionEtag;
    }

    internal long SummaryVersion
    {
        get
        {
            lock (_decisionEtagLock)
                return _summaryVersion;
        }
    }

    internal Task WaitForChangeAsync(long etag, long summaryVersion)
    {
        lock (_decisionEtagLock)
        {
            if (_lastDecisionEtag > etag || _summaryVersion > summaryVersion)
                return Task.CompletedTask;

            return _changed.Task;
        }
    }

    internal void RefreshSummary()
    {
        try
        {
            var scheduleSummary = GetScheduleSummary(DateTime.UtcNow);

            lock (_decisionEtagLock)
            {
                _scheduleSummary = scheduleSummary;
                _summaryVersion++;
                TaskExecutor.CompleteAndReplace(ref _changed);
            }
        }
        catch (Exception e)
        {
            if (_logger.IsWarnEnabled)
                _logger.Warn("Failed to refresh the backup decision log summary.", e);
        }
    }

    private void OnBackupOutcome(DatabaseBackupState backupState, BackupDecision decision)
    {
        var outcome = new BackupDecisionLogSummary.Outcome
        {
            Database = backupState.OriginalDatabaseName,
            TaskId = backupState.Configuration.TaskId,
            TaskName = backupState.Configuration.Name,
            Time = decision.LastTime,
            Error = decision.Category == BackupDecisionCategory.Failed ? decision.Reason : null
        };

        if (decision.Category == BackupDecisionCategory.Failed)
        {
            _failedCount++;
            _latestFailure = outcome;
        }
        else
        {
            _completedCount++;
            _latestCompletion = outcome;
        }
    }

    private void OnRunningBackupsChanged()
    {
        lock (_decisionEtagLock)
        {
            _summaryVersion++;
            TaskExecutor.CompleteAndReplace(ref _changed);
        }
    }

    internal BackupDecisionLogDetails GetDecisionLogDetails(BackupDecisionLogFilter filter, long? afterEtag, long afterId = 0)
    {
        var nodeTag = _serverStore.NodeTag;
        var now = DateTime.UtcNow;

        var details = new BackupDecisionLogDetails
        {
            NodeTag = nodeTag,
            RunnerStartTime = _runnerStartTime,
            Summary = GetSummary(now)
        };

        lock (_decisionEtagLock)
        {
            details.LastEtag = _lastDecisionEtag;
            details.LastId = _lastDecisionId;
        }

        var includeFilterOptions = afterEtag == null;
        var take = afterEtag == null ? filter.Take : int.MaxValue;
        var matches = new List<BackupDecisionLogEntry>();
        var databases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var tasks = new Dictionary<(string Database, long TaskId), BackupDecisionLogTask>();

        _runner._forTestingPurposes?.BeforeReadingDecisionLogs?.Invoke();

        var serverDecisions = _serverDecisionLog.GetMatchingDecisions(afterEtag, details.LastEtag, afterId, details.LastId, filter, take, out var totalResults);
        foreach (var decision in serverDecisions)
            matches.Add(BackupDecisionLogEntry.Create(decision, nodeTag));

        foreach (var (_, backupStates) in _runner.BackupsPerDatabasePerTaskId)
        {
            foreach (var (taskId, backupState) in backupStates)
            {
                if (includeFilterOptions)
                {
                    databases.Add(backupState.OriginalDatabaseName);
                    tasks.TryAdd((backupState.OriginalDatabaseName, taskId), new BackupDecisionLogTask
                    {
                        Database = backupState.OriginalDatabaseName,
                        TaskId = taskId,
                        TaskName = backupState.Configuration.Name
                    });
                }

                if (filter.MatchesTask(backupState.OriginalDatabaseName, taskId) == false)
                    continue;

                var taskDecisions = backupState.GetMatchingDecisions(afterEtag, details.LastEtag, afterId, details.LastId, filter, take, out var taskMatches);
                foreach (var decision in taskDecisions)
                    matches.Add(BackupDecisionLogEntry.Create(decision, nodeTag, backupState));

                totalResults += taskMatches;
            }
        }

        details.TotalResults = totalResults;
        details.Entries = matches
            .OrderByDescending(x => x.LastTime)
            .ThenByDescending(x => x.Id)
            .Take(take)
            .ToList();
        details.Databases = includeFilterOptions ? databases.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToList() : null;
        details.Tasks = includeFilterOptions ? tasks.Values.OrderBy(x => x.Database, StringComparer.OrdinalIgnoreCase).ThenBy(x => x.TaskId).ToList() : null;

        return details;
    }

    private BackupDecisionLogSummary GetSummary(DateTime now)
    {
        var scheduleSummary = _scheduleSummary ?? GetScheduleSummary(now);
        var counter = _serverStore.ConcurrentBackupsCounter;

        var summary = new BackupDecisionLogSummary
        {
            WaitingForSchedule = scheduleSummary.WaitingForSchedule,
            HeldBack = scheduleSummary.HeldBack,
            ServerBlock = scheduleSummary.ServerBlock
        };

        summary.Running.Current = counter.CurrentNumberOfRunningBackups;
        summary.Running.Max = counter.MaxNumberOfConcurrentBackups;
        summary.Running.WaitingForSlot = scheduleSummary.Running.WaitingForSlot;

        lock (_decisionEtagLock)
        {
            summary.Failed.Count = _failedCount;
            summary.Failed.Latest = _latestFailure;
            summary.Completed.Count = _completedCount;
            summary.Completed.Latest = _latestCompletion;
        }

        return summary;
    }

    private BackupDecisionLogSummary GetScheduleSummary(DateTime now)
    {
        var summary = new BackupDecisionLogSummary();

        var activeWindow = _serverStore.Configuration.Backup.BackupRunnerFrequency.AsTimeSpan * 2;

        BackupDecision serverDecision;
        DateTime? serverHeldUntil;
        lock (_decisionEtagLock)
        {
            _serverDecisionLog.TryGetLatest(out serverDecision);
            serverHeldUntil = _serverHeldUntil;
        }

        if (serverDecision is { Category: BackupDecisionCategory.HeldBack } &&
            (now - serverDecision.LastTime <= activeWindow || now < serverHeldUntil))
        {
            summary.ServerBlock = new BackupDecisionLogSummary.ServerBlockSummary
            {
                Detail = serverDecision.Detail,
                Reason = serverDecision.Reason,
                Since = serverDecision.FirstTime,
                Until = serverHeldUntil
            };
        }

        using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        foreach (var (_, backupStates) in _runner.BackupsPerDatabasePerTaskId)
        {
            foreach (var (taskId, backupState) in backupStates)
            {
                if (backupState.Stale)
                    continue;

                if (backupState.Running)
                    continue;

                if (backupState.TryGetLatestDecision(out var latest) && now - latest.LastTime <= activeWindow)
                {
                    if (latest.Category == BackupDecisionCategory.HeldBack)
                    {
                        summary.HeldBack.Count++;

                        if (summary.HeldBack.Longest == null || latest.FirstTime < summary.HeldBack.Longest.Since)
                        {
                            summary.HeldBack.Longest = new BackupDecisionLogSummary.HeldBackTask
                            {
                                Database = backupState.OriginalDatabaseName,
                                TaskId = taskId,
                                TaskName = backupState.Configuration.Name,
                                Detail = latest.Detail,
                                Reason = latest.Reason,
                                Since = latest.FirstTime
                            };
                        }

                        continue;
                    }

                    if (latest.Category == BackupDecisionCategory.Waiting && latest.Detail == ServerConcurrentBackupPolicy.PolicyName)
                    {
                        summary.Running.WaitingForSlot++;
                        continue;
                    }
                }

                var nextBackup = backupState.NextBackup;
                if (nextBackup == null)
                    continue;

                if (nextBackup.DateTime <= now)
                    continue;

                if (_runner.IsActiveOnThisNode(context, backupState) == false)
                    continue;

                summary.WaitingForSchedule.Count++;

                if (summary.WaitingForSchedule.NextBackupUtc == null || nextBackup.DateTime < summary.WaitingForSchedule.NextBackupUtc)
                {
                    summary.WaitingForSchedule.NextBackupUtc = nextBackup.DateTime;
                    summary.WaitingForSchedule.NextBackupDatabase = backupState.OriginalDatabaseName;
                    summary.WaitingForSchedule.NextBackupTaskName = backupState.Configuration.Name;
                }
            }
        }

        return summary;
    }
}
