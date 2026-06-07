using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.Static.Extensions;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Logging;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Backups.Policies.Database;
using Raven.Server.ServerWide.Backups.Policies.Server;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Sparrow.Server.Utils;
using Sparrow.Utils;
using Exception = System.Exception;

namespace Raven.Server.ServerWide.Backups;

/// <summary>
/// Server-level orchestrator that schedules and executes periodic backups for all databases hosted
/// on this node. A single instance lives on <see cref="ServerStore"/>, replacing the old per-database
/// <c>PeriodicBackupRunner</c>. On each polling tick the runner evaluates server-wide gate policies,
/// then iterates a shared queue of registered backup tasks and evaluates per-database policies before
/// launching each due backup on its own thread pool task. Lifecycle is tied to <see cref="ServerStore"/>.
/// </summary>
public class ServerBackupRunner : IDisposable
{

    public const int MaxDecisionLogSize = 1024; // was 32; raised for richer history during diagnostics

    // Reason strings recorded as [CANCELLED:<reason>] in the per-task decision log when an in-flight
    // backup is cancelled by a trigger. These sit alongside the inline [STARTED]/[COMPLETED]/[FAILED]
    // markers written at the RunBackup call sites below. (db-delete is intentionally absent: that path is
    // cancelled+awaited by DocumentDatabase.Dispose before RemoveDatabase runs — see RemoveDatabase.)
    // TODO (future): replace these string markers with a typed decision-log entry shape
    // (see handoff "what's next" #5 — typed decision log).
    internal const string CancelReasonDisabled = "disabled";
    internal const string CancelReasonTaskDeleted = "task-deleted";

    private readonly ServerStore _serverStore;

    private PoolOfThreads.LongRunningWork _thread;

    private readonly ConcurrentQueue<DatabaseBackupState> _backupQueue = new();

    internal readonly ConcurrentDictionary<string, ConcurrentDictionary<long, DatabaseBackupState>> BackupsPerDatabasePerTaskId = new();

    private List<IServerBackupPolicy> _serverBackupPolicies;

    private List<IDatabaseBackupPolicy> _databaseBackupPolicies;

    private readonly RavenLogger _logger;

    private readonly RavenAuditLogger _auditLog;
    private readonly List<(DateTime Time, string Reason)> _decisionLog = new();

    public ServerBackupRunner(ServerStore serverStore)
    {
        _serverStore = serverStore;
        _logger = RavenLogManager.Instance.GetLoggerForServer<ServerBackupRunner>();
        _auditLog = RavenLogManager.Instance.GetAuditLoggerForServer();
    }

    public void Initialize()
    {
        _serverBackupPolicies =
        [
            ServerStartupPolicy.Instance,
            ServerCpuCreditsPolicy.Instance,
            ServerLowMemoryPolicy.Instance,
            ServerHighDirtyMemoryPolicy.Instance,
            ClusterHealthPolicy.Instance,
            new ServerConcurrentBackupPolicy(_serverStore.ConcurrentBackupsCounter)
        ];

        _databaseBackupPolicies =
        [
            BackupRunningPolicy.Instance,
            BackupDisabledPolicy.Instance,
            DatabaseExistsPolicy.Instance,
            BackupShouldRunOnThisNodePolicy.Instance,
            BackupTimePolicy.Instance,
            DatabaseLoadedPolicy.Instance
        ];

        using (_serverStore.Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            foreach (var databaseRecord in _serverStore.Cluster.GetAllRawDatabases(context))
            {
                foreach (var periodicBackup in databaseRecord.PeriodicBackups)
                {
                    var backupState = new DatabaseBackupState(databaseRecord.DatabaseName, periodicBackup, databaseRecord.IsSharded, _serverStore);

                    RegisterNewBackup(backupState);
                }
            }
        }
        _thread = PoolOfThreads.GlobalRavenThreadPool.LongRunning(Run, null, ThreadNames.ForServerBackupRunner());
    }

    public List<(DateTime Time, string Reason)> GetDecisionLog()
    {
        lock (_decisionLog)
        {
            return new List<(DateTime Time, string Reason)>(_decisionLog);
        }
    }

    public List<(DateTime Time, string Reason)> GetDecisionLogFor(string databaseName, long taskId)
    {
        if (BackupsPerDatabasePerTaskId.TryGetValue(databaseName, out var backupsPerDatabasePerTaskId) && backupsPerDatabasePerTaskId.TryGetValue(taskId, out var backupState))
            return backupState.GetDecisionLog();

        return null;
    }

    /// <summary>
    /// Registers a new backup task state in the in-memory dictionary and enqueues it for polling.
    /// If a state for the same database already exists, uses the existing inner dictionary.
    /// </summary>
    private void RegisterNewBackup(DatabaseBackupState backupState)
    {
        if (BackupsPerDatabasePerTaskId.TryGetValue(backupState.DatabaseName, out var backupsPerDatabasePerTaskId) == false)
        {
            BackupsPerDatabasePerTaskId[backupState.DatabaseName] = backupsPerDatabasePerTaskId = new ConcurrentDictionary<long, DatabaseBackupState>();
        }

        backupsPerDatabasePerTaskId.TryAdd(backupState.Configuration.TaskId, backupState);

        _backupQueue.Enqueue(backupState);
    }

    private void Run(object state)
    {
        try
        {
            while (_serverStore.ServerShutdown.IsCancellationRequested == false)
            {
                DatabaseBackupState firstState = null;
                var now = _serverStore.Server.Time.GetUtcNow();

                while (_backupQueue.Count > 0 &&
                       _backupQueue.TryPeek(out var backupState) &&
                       backupState != firstState &&
                       AreAllServerPoliciesCompliant(now, backupState.DatabaseName) &&
                       _backupQueue.TryDequeue(out backupState))
                {
                    if (backupState.Stale) // cleanup
                        continue;

                    firstState ??= backupState;

                    bool areAllDatabasePoliciesCompliant;

                    try
                    {
                        areAllDatabasePoliciesCompliant = AreAllDatabasePoliciesCompliant(backupState, now);
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsWarnEnabled)
                            _logger.Warn($"Could not process backup policies for {backupState}.", e);

                        areAllDatabasePoliciesCompliant = false;
                    }

                    if (areAllDatabasePoliciesCompliant)
                        RunBackup(backupState, startTimeInUtc: DateTime.UtcNow);

                    _backupQueue.Enqueue(backupState); // so it will go to the end of the queue which is useful when max number of concurrent backups is set
                }

                _serverStore.ServerShutdown.WaitHandle.WaitOne(
                    _serverStore.Configuration.Backup.BackupRunnerFrequency.AsTimeSpan);
            }
        }
        catch (Exception e)
        {
            if (_logger.IsErrorEnabled)
                _logger.Error("Server backup runner has stopped due to an unexpected error.", e);

            _serverStore.NotificationCenter.Add(AlertRaised.Create(
                _serverStore.NodeTag,
                "Server Backup Runner",
                "The server backup runner has stopped unexpectedly. No backups will be scheduled until the server is restarted.",
                AlertReason.PeriodicBackup,
                NotificationSeverity.Error,
                details: new ExceptionDetails(e)));
        }
    }

    /// <summary>
    /// Launches a backup for the given state on a fire-and-forget task. Increments the concurrent
    /// backup counter before starting and releases it (via <c>FinishBackup</c>) when the backup
    /// completes or fails. A local <c>FinishBackup</c> closure resets the running flag, clears
    /// the running task, and recomputes the next scheduled time.
    /// </summary>
    private void RunBackup(DatabaseBackupState backupState, DateTime startTimeInUtc, bool isFullBackup = false, long? operationId = null)
    {
        if (_forTestingPurposes != null &&
            _forTestingPurposes.DatabaseTestingStuffInternals.TryGetValue(backupState.DatabaseName, out var testingStuffInternal))
        {
            testingStuffInternal.OnBeforeBackupStarted?.Invoke(backupState);
        }

        if (backupState.Running.Raise() == false)
            return;

        try
        {
            _serverStore.ConcurrentBackupsCounter.StartBackup(backupState.OriginalDatabaseName, backupState.Configuration.Name, _logger);
        }
        catch (BackupDelayException e)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Backup task will be retried in {(int)e.DelayPeriod.TotalSeconds} seconds, Reason: {e.Message}");

            // RavenDB-24994 (Root Cause 2): a manual "Backup Now" caller (StartBackupTask) was handed this
            // operationId and will poll /operations/state for it. Before this fix the cap-hit returned without
            // registering the operation, so the poll got 404 forever. Register the operation as Faulted with the
            // BackupDelayException payload so the poll returns a real Faulted operation carrying the rejection
            // reason. The polling loop never reaches here when capped (ServerConcurrentBackupPolicy gates it
            // first), so operationId is always supplied on this path and the database is already loaded.
            if (operationId.HasValue &&
                _serverStore.DatabasesLandlord.TryGetDatabaseIfLoaded(backupState.DatabaseName, out var database))
            {
                backupState.OperationId = operationId.Value;
                RegisterFaultedBackupOperation(database, backupState, operationId.Value, e);
            }

            backupState.Running.Lower();
            return;
        }

        backupState.AddToDecisionLog($"[STARTED] Backup task {backupState.Configuration.TaskId}", startTimeInUtc);

        _ = Task.Factory.StartNew(async () =>
        {
            try
            {
                var result = _serverStore.DatabasesLandlord.TryGetOrCreateDatabase(backupState.DatabaseName);

                DocumentDatabase database;
                switch (result.DatabaseStatus)
                {
                    case DatabasesLandlord.DatabaseSearchResult.Status.Missing:
                        FinishBackup(backupState, backupState.DatabaseName);
                        return;
                    case DatabasesLandlord.DatabaseSearchResult.Status.Database:
                        database = await result.DatabaseTask;
                        backupState.DatabaseWakeUpTimeUtc = DateTime.UtcNow;
                        break;
                    case DatabasesLandlord.DatabaseSearchResult.Status.Sharded:
                        var result2 = _serverStore.DatabasesLandlord.TryGetOrCreateShardedResourceStore(backupState.DatabaseName);
                        database = await result2;

                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                if (database.DisableOngoingTasks)
                {
                    throw new InvalidOperationException(
                        $"Backup task is disabled via marker file for database '{database.Name}'.");
                }

                var localBackupStatus = backupState.BackupStatus = backupState.GetMostUpdatedLocalBackupStatus(backupState.Configuration.TaskId, inMemoryBackupStatus: backupState.BackupStatus, database.Name);
                var backupToLocalFolder = BackupConfiguration.CanBackupUsing(backupState.Configuration.LocalSettings);
                // check if we need to do a new full backup
                if (localBackupStatus.LastFullBackup == null || // no full backup was previously performed
                    localBackupStatus.BackupType != backupState.Configuration.BackupType || // the backup type has changed
                    localBackupStatus.LastEtag == null || // last document etag wasn't updated
                    backupToLocalFolder && Documents.PeriodicBackup.BackupTask.DirectoryContainsBackupFiles(localBackupStatus.LocalBackup.BackupDirectory, IsFullBackupOrSnapshot) == false)
                // the local folder already includes a full backup or snapshot
                {
                    isFullBackup = true;
                }

                backupState.OperationId = operationId ?? database.Operations.GetNextOperationId();
                var backupTypeText = GetBackupTypeText(isFullBackup, backupState.Configuration.BackupType);

                backupState.StartTimeInUtc = startTimeInUtc;

                var backupParameters = new BackupParameters
                {
                    RetentionPolicy = backupState.Configuration.RetentionPolicy,
                    StartTimeUtc = backupState.StartTimeInUtc,
                    BackupStatus = backupState.BackupStatus,
                    IsOneTimeBackup = false,
                    IsFullBackup = isFullBackup,
                    BackupToLocalFolder = backupToLocalFolder,
                    TempBackupPath = BackupUtils.GetBackupTempPath(database.Configuration, "PeriodicBackupTemp", out _),
                    OperationId = backupState.OperationId,
                    Name = backupState.Configuration.Name
                };

                var backupTask = BackupUtils.GetBackupTask(database, backupParameters, backupState.Configuration, token: null, _logger, _forTestingPurposes);

                var tcs = new TaskCompletionSource<IOperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);

                var task = database.Operations.AddLocalOperation(
                    backupState.OperationId,
                    OperationType.DatabaseBackup,
                    $"{backupTypeText} Periodic Backup '{backupState.Configuration.Name} ({backupState.Configuration.TaskId})' for database '{backupState.DatabaseName}'.",
                    detailedDescription: null,
                    taskFactory: onProgress => StartBackupThread(database, backupState, backupTask, tcs, onProgress),
                    token: backupTask.TaskCancelToken);

                _ = task.ContinueWith(t =>
                {
                    var completionTime = DateTime.UtcNow;
                    if (t.IsFaulted)
                        backupState.AddToDecisionLog($"[FAILED] Backup task {backupState.Configuration.TaskId}: {t.Exception?.Flatten().InnerException?.Message ?? "unknown error"}", completionTime);
                    else if (t.IsCanceled == false)
                        backupState.AddToDecisionLog($"[COMPLETED] Backup task {backupState.Configuration.TaskId}", completionTime);

                    backupTask.TaskCancelToken.Dispose();
                    FinishBackup(backupState, backupState.DatabaseName);

                    if (_forTestingPurposes != null &&
                        _forTestingPurposes.DatabaseTestingStuffInternals.TryGetValue(backupState.DatabaseName, out var afterTestingStuff))
                    {
                        afterTestingStuff.OnAfterBackupCompleted?.Invoke(backupState);
                    }
                });


            }
            catch (Exception e)
            {
                backupState.AddToDecisionLog($"[FAILED] Backup task {backupState.Configuration.TaskId}: {e.Message}", DateTime.UtcNow);

                if (_logger.IsErrorEnabled)
                    _logger.Error($"Could not start backup for {backupState}.", e);

                FinishBackup(backupState, backupState.DatabaseName);
            }
        });

        void FinishBackup(DatabaseBackupState databaseBackupState, string databaseName)
        {
            if (_forTestingPurposes != null &&
                _forTestingPurposes.DatabaseTestingStuffInternals != null &&
                _forTestingPurposes.DatabaseTestingStuffInternals.TryGetValue(databaseName, out TestingStuffInternal testingStuffInternal))
            {
                testingStuffInternal.HoldBackupFromFinishing?.WaitOne();
            }

            backupState.NextBackup = backupState.GetNextBackupDetails(out string tag);
            databaseBackupState.RunningTask = null;

            backupState.Running.Lower();
            _serverStore.ConcurrentBackupsCounter.FinishBackup(databaseBackupState.OriginalDatabaseName, databaseBackupState.Configuration.Name, databaseBackupState.RunningBackupStatus, null, _logger);
            databaseBackupState.RunningBackupStatus = null;

            // Null the cancellation handle last — after Running.Lower() and RunningTask = null — so a
            // trigger calling CancelRunningBackup that races FinishBackup sees either "still running, token
            // live" or "stopped, token cleared", never a dangling reference to a token we just disposed in
            // the operation continuation. Mirrors the OnGoingBackup snapshot-guard ordering.
            databaseBackupState.RunningCancel = null;
        }
    }

    /// <summary>
    /// Registers <paramref name="operationId"/> in the database's operations registry as an already-Faulted
    /// operation carrying <paramref name="exception"/>. Used when the concurrent-backup cap rejects a manual
    /// backup so that the caller polling /operations/state sees a real Faulted operation with the rejection
    /// reason instead of a 404 (RavenDB-24994, Root Cause 2). The faulted task factory makes the operation
    /// settle straight to Faulted via the standard operations completion path (no concurrent-backup slot is
    /// taken, so no matching FinishBackup is required).
    /// </summary>
    private static void RegisterFaultedBackupOperation(DocumentDatabase database, DatabaseBackupState backupState, long operationId, BackupDelayException exception)
    {
        _ = database.Operations.AddLocalOperation(
            operationId,
            OperationType.DatabaseBackup,
            $"Periodic Backup '{backupState.Configuration.Name} ({backupState.Configuration.TaskId})' for database '{backupState.DatabaseName}'.",
            detailedDescription: null,
            taskFactory: _ => Task.FromException<IOperationResult>(exception),
            token: null);
    }

    /// <summary>
    /// Triggers an immediate (manual) backup for the given task, using a pre-allocated
    /// <paramref name="operationId"/>. Throws if the task doesn't exist or is already running.
    /// Used by the "backup now" API endpoint.
    /// </summary>
    public long StartBackupTask(string databaseName, long taskId, bool isFullBackup, long operationId, DateTime? startTimeUtc = null)
    {
        DatabaseBackupState databaseBackupState = GetDatabaseStateByTaskId(databaseName, taskId);
        if (databaseBackupState == null)
        {
            throw new InvalidOperationException($"Backup task id: {taskId} doesn't exist");
        }
        if (databaseBackupState.Running)
        {
            throw new BackupAlreadyRunningException(
                $"Could not start backup task '{databaseBackupState.Configuration.TaskId}' because there is already a running backup under operation id '{databaseBackupState.RunningTask.Id}'")
            {
                OperationId = databaseBackupState.RunningTask.Id,
                NodeTag = _serverStore.NodeTag
            };
        }
        RunBackup(databaseBackupState, startTimeUtc ?? SystemTime.UtcNow, isFullBackup, operationId);
        return operationId;
    }

    /// <summary>
    /// Spawns a dedicated thread for the backup work and wires up the completion source so the
    /// operation-tracking infrastructure gets notified when the thread finishes.
    /// </summary>
    private Task<IOperationResult> StartBackupThread(DocumentDatabase database, DatabaseBackupState backupState, Documents.PeriodicBackup.BackupTask backupTask, TaskCompletionSource<IOperationResult> tcs, Action<IOperationProgress> onProgress)
    {
        var threadName = $"Backup task {backupState.Configuration.Name} for database '{database.Name}'";
        PoolOfThreads.GlobalRavenThreadPool.LongRunning(x => RunBackupThread(database, backupState, backupTask, threadName, tcs, onProgress), null, ThreadNames.ForBackupTask(threadName,
            database.Name, backupState.Configuration.Name));
        return tcs.Task;
    }

    /// <summary>
    /// Executes the backup on the current thread, populates <c>runningBackupStatus</c> from the
    /// prior status, then calls <see cref="BackupTask.RunPeriodicBackup"/>. Handles cancellation
    /// (delayed backups) and unexpected exceptions, propagating the outcome through the TCS.
    /// </summary>
    private void RunBackupThread(DocumentDatabase database, DatabaseBackupState backupState, Documents.PeriodicBackup.BackupTask backupTask, string threadName, TaskCompletionSource<IOperationResult> tcs, Action<IOperationProgress> onProgress)
    {
        BackupResult backupResult = null;
        var runningBackupStatus = new PeriodicBackupStatus
        {
            TaskId = backupState.Configuration.TaskId,
            BackupType = backupState.Configuration.BackupType,
            LastEtag = backupState.BackupStatus.LastEtag,
            LastRaftIndex = backupState.BackupStatus.LastRaftIndex,
            LastFullBackup = backupState.BackupStatus.LastFullBackup,
            LastIncrementalBackup = backupState.BackupStatus.LastIncrementalBackup,
            LastFullBackupInternal = backupState.BackupStatus.LastFullBackupInternal,
            LastIncrementalBackupInternal = backupState.BackupStatus.LastIncrementalBackupInternal,
            IsFull = backupTask._isFullBackup,
            LocalBackup = backupState.BackupStatus.LocalBackup,
            LastOperationId = backupState.BackupStatus.LastOperationId,
            FolderName = backupState.BackupStatus.FolderName,
            LastDatabaseChangeVector = backupState.BackupStatus.LastDatabaseChangeVector
        };

        backupState.RunningBackupStatus = runningBackupStatus;

        try
        {
            ThreadHelper.TrySetThreadPriority(ThreadPriority.BelowNormal, threadName, _logger);
            NativeMemory.EnsureRegistered();

            using (database.PreventFromUnloadingByIdleOperations())
            {
                backupResult = backupTask.RunPeriodicBackup(onProgress, backupState, tcs.Task, ref runningBackupStatus);

                backupState.BackupStatus = runningBackupStatus;

                tcs.SetResult(backupResult);
            }

            if (RavenLogManager.Instance.IsAuditEnabled)
            {
                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    var backupKind = backupTask._isFullBackup ? BackupKind.Full : BackupKind.Incremental;
                    var configurationString = context.ReadObject(backupState.Configuration.ToAuditJson(), nameof(PeriodicBackupConfiguration)).ToString();
                    _auditLog.Audit($"BACKUP {backupKind} backup executed automatically as scheduled with configuration: '{configurationString}'");
                }
            }
        }
        catch (Exception e) when (e.ExtractSingleInnerException() is OperationCanceledException oce)
        {
            var inMemoryBackupStatus = GetDatabaseStateByTaskId(database.Name, backupState.BackupStatus.TaskId);
            if (inMemoryBackupStatus != null)
            {
                runningBackupStatus.DelayUntil = inMemoryBackupStatus.BackupStatus.DelayUntil;
                runningBackupStatus.OriginalBackupTime = inMemoryBackupStatus.BackupStatus.OriginalBackupTime;
            }

            if (_logger.IsInfoEnabled)
                _logger.Info($"Canceled the backup thread: '{backupState.Configuration.Name}'", oce);

            backupState.BackupStatus = runningBackupStatus;

            tcs.SetCanceled();
        }
        catch (Exception e)
        {
            if (_logger.IsErrorEnabled)
                _logger.Error($"Failed to run the backup thread: '{backupState.Configuration.Name}'", e);

            backupState.BackupStatus = runningBackupStatus;

            tcs.SetException(e);
        }
    }

    /// <summary>
    /// Delays an in-progress backup by sending a cluster-wide <see cref="DelayBackupCommand"/> and
    /// then killing the running operation so the backup thread exits cleanly. Updates in-memory state
    /// after the cluster command commits so that the next polling cycle sees the correct next-run time.
    /// </summary>
    public async Task DelayAsync(string databaseName, long operationId, DateTime delayUntil, X509Certificate2 clientCert, CancellationToken token)
    {
        if (BackupsPerDatabasePerTaskId.TryGetValue(databaseName, out var backupsPerDatabasePerTaskId) == false)
            throw new InvalidOperationException($"Fail to delay backup task with operationId '{operationId}',  the operation with that number isn't registered");

        foreach (var databaseBackupState in backupsPerDatabasePerTaskId)
        {
            var runningTask = databaseBackupState.Value.RunningTask;
            if (runningTask == null || runningTask.Id != operationId)
                continue;

            var nextBackup = databaseBackupState.Value.GetNextBackupDetails(out string tag);

            var originalBackupTime = delayUntil > nextBackup.DateTime ? nextBackup.DateTime : databaseBackupState.Value.StartTimeInUtc;

            var command = new DelayBackupCommand(databaseName, RaftIdGenerator.NewId())
            {
                TaskId = databaseBackupState.Key,
                DelayUntil = delayUntil,
                OriginalBackupTime = originalBackupTime
            };

            try
            {
                (long index, _) = await _serverStore.SendToLeaderAsync(command);
                await _serverStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, index);
            }
            catch (Exception e)
            {
                if (_logger.IsWarnEnabled)
                {
                    var msg =
                        $"Fail to delay backup task with task id '{operationId}' cluster-wide, the task was delayed until '{delayUntil}' UTC only on the current node.";

                    _logger.Warn(msg, e);
                }
            }
            databaseBackupState.Value.BackupStatus.DelayUntil = delayUntil;
            databaseBackupState.Value.BackupStatus.OriginalBackupTime = originalBackupTime;
            databaseBackupState.Value.NextBackup = databaseBackupState.Value.GetNextBackupDetails(out string nodeTag);

            if (_forTestingPurposes != null &&
                _forTestingPurposes.DatabaseTestingStuffInternals != null &&
                _forTestingPurposes.DatabaseTestingStuffInternals.TryGetValue(databaseName, out ServerBackupRunner.TestingStuffInternal testingStuffInternal))
            {
                testingStuffInternal.OnBackupTaskRunHoldBackupExecution?.SetResult(null);
            }

            var database = await _serverStore.DatabasesLandlord.TryGetOrCreateDatabase(databaseBackupState.Value.DatabaseName).DatabaseTask;

            await database.Operations.KillOperationAsync(operationId, token);

            try
            {
                // use the local `runningTask` captured before KillOperationAsync — `databaseBackupState.Value.RunningTask`
                // is cleared by FinishBackup as soon as KillOperationAsync returns, so reading it here would NRE
                await runningTask.Task;
            }
            catch
            {
                // task has ended, nothing we can do here
            }


            return;
        }

        throw new InvalidOperationException($"Fail to delay backup task with task id '{operationId}', the operation with that number isn't registered");
    }

    /// <summary>
    /// Returns the most current <see cref="PeriodicBackupStatus"/> for a task by merging the in-memory
    /// state with the cluster value store, preferring whichever is more recent. Used by ongoing-task
    /// read paths (Studio, API) that need a consistent view across nodes.
    /// </summary>
    public PeriodicBackupStatus GetMostUpdatedClusterBackupStatus(string databaseName, long taskId)
    {
        PeriodicBackupStatus inMemoryBackupStatus = null;
        var databaseBackupState = GetDatabaseStateByTaskId(databaseName, taskId);
        if (databaseBackupState != null)
        {
            inMemoryBackupStatus = databaseBackupState.BackupStatus;
        }

        if (_forTestingPurposes != null &&
            _forTestingPurposes.DatabaseTestingStuffInternals != null &&
            _forTestingPurposes.DatabaseTestingStuffInternals.TryGetValue(databaseName, out ServerBackupRunner.TestingStuffInternal testingStuffInternal))
        {
            if (testingStuffInternal.BackupStatusFromMemoryOnly)
                return inMemoryBackupStatus;
        }

        using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            var backupStatus = BackupUtils.GetBackupStatusFromCluster(context, databaseName, taskId);
            return BackupUtils.ComparePeriodicBackupStatus(taskId, backupStatus, inMemoryBackupStatus);
        }
    }

    /// <summary>
    /// Reacts to cluster value-change notifications for a database. Handles
    /// <see cref="UpdateResponsibleNodeForTasksCommand"/> by refreshing configurations, and
    /// <see cref="DelayBackupCommand"/> by writing the new <c>DelayUntil</c> /
    /// <c>OriginalBackupTime</c> into the in-memory <see cref="DatabaseBackupState.BackupStatus"/>
    /// so that consumers running with <c>BackupStatusFromMemoryOnly = true</c> (e.g.
    /// <c>EveryNodeHasDelayInMemory</c>) see the delay on every peer. Cluster-persisted readers
    /// pick up the same values from the durable rows written by <c>DelayBackupCommand.Execute</c>.
    /// </summary>
    public void HandleDatabaseValueChanged(string type, object changeState, string databaseName)
    {
        switch (type)
        {
            case nameof(UpdateResponsibleNodeForTasksCommand):
                using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (context.OpenReadTransaction())
                using (var rawRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, databaseName))
                {
                    UpdateConfigurations(rawRecord.PeriodicBackups, databaseName);
                }
                break;

            case nameof(DelayBackupCommand):
                var state = (DelayBackupCommand.DelayBackupCommandState)changeState;

                var databaseBackupState = GetDatabaseStateByTaskId(databaseName, state.TaskId);
                if (databaseBackupState == null)
                    throw new InvalidOperationException($"Backup task id: {state.TaskId} doesn't exist");

                // In-memory propagation for BackupStatusFromMemoryOnly = true consumers
                // (M9, reinstated in 5.11 — see docs/backup-refactor-fix-audit.md Round 2).
                // The cluster-persisted path handles all other scenarios.
                // Include TaskId so the synthesized fallback never produces a status with TaskId = 0
                // that downstream readers (e.g. RunBackupThread line 431) would consume.
                databaseBackupState.BackupStatus ??= new PeriodicBackupStatus { TaskId = state.TaskId };
                databaseBackupState.BackupStatus.DelayUntil = state.DelayUntil;
                databaseBackupState.BackupStatus.OriginalBackupTime = state.OriginalBackupTime;
                break;
        }
    }

    public void HandleDatabaseRecordChange(RawDatabaseRecord databaseRecord)
    {
        if (databaseRecord.PeriodicBackups == null || databaseRecord.PeriodicBackups.Count == 0)
        {
            if (this.BackupsPerDatabasePerTaskId.TryGetValue(databaseRecord.DatabaseName, out var backupsPerDatabasePerTaskId))
            {
                foreach (var kvp in backupsPerDatabasePerTaskId.ForceEnumerateInThreadSafeManner())
                {
                    kvp.Value.Stale.Raise();
                    // The record no longer has any backup tasks — every previously-registered task was
                    // removed. Cancel any in-flight backup so it does not run to completion and write a
                    // status row for a task that no longer exists.
                    kvp.Value.CancelRunningBackup(CancelReasonTaskDeleted);
                }

                // queue will clear itself
                backupsPerDatabasePerTaskId.Clear();
            }

            return;
        }

        if (BackupsPerDatabasePerTaskId.TryGetValue(databaseRecord.DatabaseName, out var backupsPerDatabase) == false)
            BackupsPerDatabasePerTaskId[databaseRecord.DatabaseName] = backupsPerDatabase = new ConcurrentDictionary<long, DatabaseBackupState>();

        foreach (var periodicBackup in databaseRecord.PeriodicBackups)
        {
            if (backupsPerDatabase.TryGetValue(periodicBackup.TaskId, out var currentBackupState))
                UpdateConfigurations([periodicBackup], databaseRecord.DatabaseName);
            else
            {
                var newBackupState = new DatabaseBackupState(databaseRecord.DatabaseName, periodicBackup, databaseRecord.Sharding != null, _serverStore);

                RegisterNewBackup(newBackupState);
            }
        }

        foreach (var removedTaskId in backupsPerDatabase.Keys.Except(databaseRecord.PeriodicBackupsTaskIds))
        {
            if (backupsPerDatabase.TryRemove(removedTaskId, out var backupState))
            {
                backupState.Stale.Raise();
                backupState.CancelRunningBackup(CancelReasonTaskDeleted);
            }
        }
    }

    /// <summary>
    /// Applies a new list of backup configurations for a database, adding tasks that are new,
    /// updating existing ones, and marking removed tasks as stale so the queue discards them.
    /// </summary>
    public void UpdateConfigurations(List<PeriodicBackupConfiguration> configurations, string databaseName)
    {
        if (BackupsPerDatabasePerTaskId.TryGetValue(databaseName, out ConcurrentDictionary<long, DatabaseBackupState> backupStates) == false)
            throw new InvalidOperationException($"There isn't backups configuration for Database : {databaseName} ");

        if (configurations == null)
        {
            foreach (var kvp in backupStates.ForceEnumerateInThreadSafeManner())
            {
                kvp.Value.Stale.Raise();
                kvp.Value.CancelRunningBackup(CancelReasonTaskDeleted);
            }
            // queue will clear itself
            backupStates.Clear();
            return;
        }

        var allBackupTaskIds = new List<long>();
        foreach (var periodicBackupConfiguration in configurations)
        {
            var newBackupTaskId = periodicBackupConfiguration.TaskId;
            allBackupTaskIds.Add(newBackupTaskId);

            var taskState = GetTaskStatus(periodicBackupConfiguration, databaseName, out _);
            if (_forTestingPurposes != null &&
                _forTestingPurposes.DatabaseTestingStuffInternals != null &&
                _forTestingPurposes.DatabaseTestingStuffInternals.TryGetValue(databaseName, out ServerBackupRunner.TestingStuffInternal testingStuffInternal))
            {
                if (testingStuffInternal.SimulateActiveByOtherNodeStatus_UpdateConfigurations)
                {
                    taskState = TaskStatus.ActiveByOtherNode;
                }
                else if (testingStuffInternal.SimulateDisableNodeStatus_UpdateConfigurations)
                {
                    taskState = TaskStatus.Disabled;
                }
                else if (testingStuffInternal.SimulateActiveByCurrentNode_UpdateConfigurations)
                {
                    taskState = TaskStatus.ActiveByCurrentNode;
                }
            }

            UpdatePeriodicBackup(databaseName, newBackupTaskId, periodicBackupConfiguration, taskState);
        }

        var deletedBackupTaskIds = backupStates.Keys.Except(allBackupTaskIds).ToList();

        foreach (var deletedBackupId in deletedBackupTaskIds)
        {
            backupStates[deletedBackupId].Stale.Raise();
            backupStates[deletedBackupId].CancelRunningBackup(CancelReasonTaskDeleted);
        }
    }

    /// <summary>
    /// Updates a single backup task's in-memory state when its configuration or responsible-node
    /// assignment changes. Marks the task stale when it becomes disabled or moves to another node;
    /// re-enqueues it and refreshes the next-run time when it stays active on this node.
    /// </summary>
    private void UpdatePeriodicBackup(string databaseName, long taskId,
        PeriodicBackupConfiguration newConfiguration,
        TaskStatus taskState)
    {
        Debug.Assert(taskId == newConfiguration.TaskId);

        var name = databaseName;
        var existingBackupState = GetDatabaseStateByTaskId(name, taskId);

        // Creating a new backup state
        if (existingBackupState == null)
        {
            var backupState = new DatabaseBackupState(databaseName, newConfiguration, ShardHelper.IsShardName(databaseName), _serverStore);
            RegisterNewBackup(backupState);
            return;
        }

        var previousConfiguration = existingBackupState.Configuration;
        existingBackupState.Configuration = newConfiguration;

        if (BackupHelper.BackupTypeChanged(previousConfiguration, newConfiguration))
        {
            // BackupTypeChanged forces the next backup to be full — the BackupType mismatch
            // at the IsFullBackup-decision read (RunBackup line 262-268) handles that.
            // Reset rather than null so concurrent readers (HandleDatabaseValueChanged,
            // DelayAsync, RunBackupThread cancel path) never see a null BackupStatus.
            existingBackupState.BackupStatus = new PeriodicBackupStatus
            {
                TaskId = newConfiguration.TaskId,
                BackupType = newConfiguration.BackupType
            };
        }

        switch (taskState)
        {
            case TaskStatus.Disabled:
                existingBackupState.Stale.Raise();
                existingBackupState.CancelRunningBackup(CancelReasonDisabled);
                if (_logger.IsDebugEnabled)
                    _logger.Debug($"Backup task '{taskId}' state is '{taskState}', will cancel the backup for it.");

                return;
            case TaskStatus.ActiveByOtherNode:
                existingBackupState.Stale.Raise();
                if (_logger.IsDebugEnabled)
                    _logger.Debug($"Backup task '{taskId}' state is '{taskState}', will skip polling for it.");

                return;

            case TaskStatus.MissingResponsibleNode:
                // the responsible node wasn't determined yet
                return;

            case TaskStatus.ActiveByCurrentNode:
                if (existingBackupState.Stale)
                {
                    existingBackupState.Stale.Lower();
                    _backupQueue.Enqueue(existingBackupState);
                }

                // a backup is already running, the next one will be re-scheduled by the backup task if needed
                if (existingBackupState.RunningTask != null)
                {
                    if (_logger.IsDebugEnabled)
                        _logger.Debug($"Backup task '{taskId}' state is '{taskState}', and currently are being executed since '{existingBackupState.StartTimeInUtc}'.");
                    return;
                }

                // backup frequency hasn't changed, and we have a scheduled backup
                if (previousConfiguration.HasBackupFrequencyChanged(newConfiguration) == false)
                {
                    if (_logger.IsDebugEnabled)
                        _logger.Debug($"Backup task '{taskId}' state is '{taskState}', the task doesn't have frequency changes and has scheduled backup, will continue to execute by the current node '{_serverStore.NodeTag}'.");

                    return;
                }

                if (_logger.IsDebugEnabled)
                    _logger.Debug($"Backup task '{taskId}' state is '{taskState}', the task has frequency changes or doesn't have scheduled backup, the timer will be rearranged and the task will be executed by current node '{_serverStore.NodeTag}'.");


                existingBackupState.NextBackup = existingBackupState.GetNextBackupDetails(out string tag);

                return;

            default:
                throw new ArgumentOutOfRangeException(nameof(taskState), taskState, null);
        }
    }

    /// <summary>
    /// Determines the scheduling status of a backup task for the given database: disabled, active on
    /// this node, active on another node, or missing a responsible-node assignment. Also returns the
    /// responsible node tag when one is assigned.
    /// </summary>
    public TaskStatus GetTaskStatus(PeriodicBackupConfiguration configuration, string databaseName, out string responsibleNodeTag, bool disableLog = false)
    {
        if (configuration.Disabled)
        {
            responsibleNodeTag = null;
            return TaskStatus.Disabled;
        }

        if (configuration.HasBackup() == false)
        {
            if (disableLog == false)
            {
                var message = $"All backup destinations are disabled for backup task id: {configuration.TaskId}";
                _serverStore.NotificationCenter.Add(AlertRaised.Create(
                    databaseName,
                    "Periodic Backup",
                    message,
                    AlertReason.PeriodicBackup,
                    NotificationSeverity.Info));
            }

            responsibleNodeTag = null;
            return TaskStatus.Disabled;
        }

        responsibleNodeTag = BackupUtils.GetResponsibleNodeTag(_serverStore, databaseName, configuration.TaskId);
        if (responsibleNodeTag == null)
        {
            // the responsible node wasn't set by the cluster observer yet
            if (_forTestingPurposes != null &&
                _forTestingPurposes.DatabaseTestingStuffInternals != null &&
                _forTestingPurposes.DatabaseTestingStuffInternals.TryGetValue(databaseName, out ServerBackupRunner.TestingStuffInternal testingStuffInternal))
            {
                testingStuffInternal.OnMissingResponsibleNode?.Invoke();
            }

            return TaskStatus.MissingResponsibleNode;
        }

        if (responsibleNodeTag == _serverStore.NodeTag)
            return TaskStatus.ActiveByCurrentNode;

        if (disableLog == false && _logger.IsDebugEnabled)
            _logger.Debug($"Backup job is skipped at {SystemTime.UtcNow}, because it is managed " +
                          $"by '{responsibleNodeTag}' node and not the current node ({_serverStore.NodeTag})");

        return TaskStatus.ActiveByOtherNode;
    }

    private static string GetBackupTypeText(bool isFullBackup, BackupType backupType)
    {
        if (backupType == BackupType.Backup)
        {
            return isFullBackup ? "Full" : "Incremental";
        }

        return isFullBackup ? "Snapshot" : "Incremental Snapshot";
    }

    private static bool IsFullBackupOrSnapshot(string filePath)
    {
        var extension = Path.GetExtension(filePath);
        return Constants.Documents.PeriodicBackup.FullBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
               Constants.Documents.PeriodicBackup.SnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
               Constants.Documents.PeriodicBackup.EncryptedFullBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
               Constants.Documents.PeriodicBackup.EncryptedSnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Returns the node tag of the cluster member responsible for executing the given backup task.
    /// Throws when the task doesn't exist, is disabled, or has all destinations turned off.
    /// </summary>
    public string WhoseTaskIsIt(string databaseName, long taskId)
    {
        DatabaseBackupState databaseBackupState = GetDatabaseStateByTaskId(databaseName, taskId);
        if (databaseBackupState == null)
        {
            throw new InvalidOperationException($"Backup task id: {taskId} doesn't exist");
        }

        if (databaseBackupState.Configuration.Disabled)
        {
            throw new InvalidOperationException($"Backup task id: {taskId} is disabled");
        }

        if (databaseBackupState.Configuration.HasBackup() == false)
        {
            throw new InvalidOperationException($"All backup destinations are disabled for backup task id: {taskId}");
        }

        return BackupUtils.GetResponsibleNodeTag(_serverStore, databaseBackupState.DatabaseName, databaseBackupState.Configuration.TaskId);
    }

    /// <summary>Returns the in-memory backup state for a specific (database, task-id) pair, or null if not registered.</summary>
    public DatabaseBackupState GetDatabaseStateByTaskId(string databaseName, long taskId)
    {
        if (BackupsPerDatabasePerTaskId.TryGetValue(databaseName, out var backupsPerDatabasePerTaskId) &&
             backupsPerDatabasePerTaskId.TryGetValue(taskId, out var databaseBackupState))
        {
            return databaseBackupState;
        }

        return null;
    }

    /// <summary>Returns all registered backup states for a database, or an empty list if the database has none.</summary>
    public List<DatabaseBackupState> GetDatabaseBackups(string databaseName)
    {
        return BackupsPerDatabasePerTaskId.TryGetValue(databaseName, out var backupsPerDatabasePerTaskId) ?
            backupsPerDatabasePerTaskId.Values.ToList() : [];
    }

    /// <summary>
    /// Returns the next scheduled backup time for the given task by delegating to the task's
    /// <see cref="DatabaseBackupState"/>. Also returns the responsible node tag via <paramref name="tag"/>.
    /// </summary>
    public NextBackup GetNextBackupDetails(long taskId, string databaseName, out string tag)
    {
        var state = GetDatabaseStateByTaskId(databaseName, taskId);
        var next = state.GetNextBackupDetails(out tag);
        return next;
    }

    /// <summary>
    /// Describes the scheduling role of this node for a particular backup task.
    /// Used by <see cref="GetTaskStatus"/> and policy evaluators to decide whether to run,
    /// skip, or remove the task from the queue.
    /// </summary>
    public enum TaskStatus
    {
        Disabled,
        ActiveByCurrentNode,
        ActiveByOtherNode,
        MissingResponsibleNode
    }

    /// <summary>Returns aggregated backup information for a database, opening a read transaction internally.</summary>
    public BackupInfo GetBackupInfo(string databaseName)
    {
        using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            return GetBackupInfo(context, databaseName);
        }
    }

    /// <summary>Returns aggregated backup information for a database using the supplied read transaction.</summary>
    public BackupInfo GetBackupInfo(ClusterOperationContext context, string databaseName)
    {
        List<DatabaseBackupState> BackupStates = null;
        if (BackupsPerDatabasePerTaskId.TryGetValue(databaseName, out ConcurrentDictionary<long, DatabaseBackupState> databaseBackupStates))
        {
            BackupStates = databaseBackupStates.Values.ToList();
        }

        return BackupUtils.GetBackupInfo(
            new BackupUtils.BackupInfoParameters
            {
                Context = context,
                DatabaseBackupStates = BackupStates,
                DatabaseName = databaseName
            }
        );
    }

    private bool AreAllServerPoliciesCompliant(DateTime now, string databaseName)
    {
        foreach (var backupPolicy in _serverBackupPolicies)
        {
            if (backupPolicy.CanDoBackup(_serverStore, now, databaseName, out string reason) == false)
            {
                if (_logger.IsDebugEnabled)
                    _logger.Debug(reason);

                AddToDecisionLog(reason, now);

                if (_forTestingPurposes != null &&
                    _forTestingPurposes.DatabaseTestingStuffInternals.TryGetValue(databaseName, out var serverViolationTestingStuff))
                {
                    serverViolationTestingStuff.OnServerPolicyViolation?.Invoke(backupPolicy, reason);
                }

                return false;
            }
        }

        return true;
    }

    private bool AreAllDatabasePoliciesCompliant(DatabaseBackupState backupState, DateTime now)
    {
        if (_forTestingPurposes != null &&
            _forTestingPurposes.DatabaseTestingStuffInternals != null &&
            _forTestingPurposes.DatabaseTestingStuffInternals.TryGetValue(backupState.DatabaseName, out var testingStuffInternal) &&
            testingStuffInternal.SimulateActiveByOtherNodeStatus_Reschedule)
        {
            var reason = $"Cannot start backup {backupState} because SimulateActiveByOtherNodeStatus_Reschedule is set.";
            backupState.AddToDecisionLog(reason, now);
            return false;
        }

        using (_serverStore.Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            foreach (var backupPolicy in _databaseBackupPolicies)
            {
                if (backupPolicy.CanDoBackup(context, _serverStore, backupState, now, out string reason) == false)
                {
                    if (_logger.IsDebugEnabled)
                        _logger.Debug(reason);

                    backupState.AddToDecisionLog(reason, now);

                    if (_forTestingPurposes != null &&
                        _forTestingPurposes.DatabaseTestingStuffInternals.TryGetValue(backupState.DatabaseName, out var dbViolationTestingStuff))
                    {
                        dbViolationTestingStuff.OnDatabasePolicyViolation?.Invoke(backupPolicy, reason, backupState);
                    }

                    return false;
                }
            }

            return true;
        }
    }

    public void Dispose()
    {
        _thread.Join(int.MaxValue);

        // collect in-flight backup tasks
        var runningTasks = new List<Task>();
        foreach (var backupsPerTask in BackupsPerDatabasePerTaskId.Values)
        {
            foreach (var kvp in backupsPerTask.ForceEnumerateInThreadSafeManner())
            {
                kvp.Value.Stale.Raise();
                var runningTask = kvp.Value.RunningTask?.Task;
                if (runningTask != null)
                    runningTasks.Add(runningTask);
            }

            backupsPerTask.Clear();
        }

        BackupsPerDatabasePerTaskId.Clear();

        if (runningTasks.Count > 0)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Waiting for {runningTasks.Count} running backup(s) to complete.");

            Task.WaitAll(runningTasks.ToArray(), TimeSpan.FromMinutes(5));
        }
    }

    /// <summary>
    /// Deregisters all backup tasks for a database being unloaded or deleted, marking each state
    /// stale so the polling loop discards it without attempting to start any more backups.
    /// </summary>
    public void RemoveDatabase(string databaseName)
    {
        if (BackupsPerDatabasePerTaskId.TryRemove(databaseName, out var backupsPerTaskId))
        {
            foreach (var kvp in backupsPerTaskId.ForceEnumerateInThreadSafeManner())
            {
                kvp.Value.Stale.Raise();
            }

            backupsPerTaskId.Clear();
        }

        // Note: no explicit CancelRunningBackup here. On db-delete the in-flight backup is already
        // cancelled AND awaited to completion by DocumentDatabase.Dispose before this runs: the backup
        // token is linked to Database.DatabaseShutdown (BackupTask.cs), and DisposeInternal cancels it
        // (_databaseShutdown.Cancel()) then disposes the operations registry — Operations.Dispose calls
        // KillAsync(waitForCompletion: true) on the killable backup operation, which runs BEFORE
        // DocumentsStorage is disposed. So the backup observes cancellation while storage is still alive
        // (clean OperationCanceledException, no status row) and has finished by the time RemoveDatabase is
        // invoked (DatabasesLandlord.DeleteDatabase disposes the database first, then calls this). An
        // explicit cancel here would be a no-op: RunningCancel is already null (cleared in FinishBackup).

        // A5b: drop any test-hook entry tied to this database, so a callback held by a
        // disposed test resource cannot fire if another test re-uses this server. Safe
        // no-op in production (the field is null) and when no hook was installed.
        _forTestingPurposes?.DatabaseTestingStuffInternals?.TryRemove(databaseName, out _);
    }

    /// <summary>
    /// Returns a snapshot of the currently running backup for the given task, or null if no backup
    /// is in progress. Used by the ongoing-tasks endpoint to populate the "running" indicator in Studio.
    /// </summary>
    public RunningBackup OnGoingBackup(string databaseName, long taskId)
    {
        var state = GetDatabaseStateByTaskId(databaseName, taskId);
        if (state == null)
            return null;

        // Snapshot RunningTask once: FinishBackup clears RunningTask before lowering Running,
        // so a reader could otherwise see Running == true with RunningTask == null and NRE.
        var runningTask = state.RunningTask;
        if (state.Running == false || runningTask == null)
            return null;

        return new RunningBackup
        {
            StartTime = state.StartTimeInUtc,
            IsFull = state.RunningBackupStatus?.IsFull ?? false,
            RunningBackupTaskId = runningTask.Id
        };
    }

    public void AddToDecisionLog(string reason, DateTime now)
    {
        lock (_decisionLog)
        {
            _decisionLog.Insert(0, (now, reason));

            if (_decisionLog.Count > MaxDecisionLogSize)
                _decisionLog.RemoveAt(_decisionLog.Count - 1);
        }
    }

    /// <summary>
    /// Returns a projection of all registered backup tasks for a database, used by the
    /// backup timers debug endpoint to display scheduling information.
    /// </summary>
    internal List<PeriodicBackupInfo> GetPeriodicBackupsInformation(string databaseName)
    {
        if (BackupsPerDatabasePerTaskId.TryGetValue(databaseName, out var backupsPerDatabase) == false)
            return new List<PeriodicBackupInfo>();

        return backupsPerDatabase.Values
            .Select(x => new PeriodicBackupInfo
            {
                Database = databaseName,
                TaskId = x.Configuration.TaskId,
                Name = x.Configuration.Name,
                FullBackupFrequency = x.Configuration.FullBackupFrequency,
                IncrementalBackupFrequency = x.Configuration.IncrementalBackupFrequency,
                NextBackup = x.NextBackup,
                CreatedAt = x.CreatedAt,
            })
            .ToList();
    }

    /// <summary>
    /// Pairs the operation ID with the underlying <see cref="Task"/> for a currently executing backup.
    /// Stored on <see cref="DatabaseBackupState.RunningTask"/> and cleared by the finish callback.
    /// </summary>
    public sealed class RunningBackupTask
    {
        public Task Task { get; set; }

        public long Id { get; set; }
    }


    internal TestingStuff _forTestingPurposes;

    internal TestingStuff ForTestingPurposesOnly()
    {
        if (_forTestingPurposes != null)
            return _forTestingPurposes;

        return _forTestingPurposes = new TestingStuff();
    }

    /// <summary>
    /// Test-only hook container attached to <see cref="ServerBackupRunner"/> when a test calls
    /// <see cref="ForTestingPurposesOnly"/>. All members on this class are for test control only;
    /// none are read in production (the field is null in production code).
    /// </summary>
    public class TestingStuff
    {
        public TestingStuff()
        {
            DatabaseTestingStuffInternals = new ConcurrentDictionary<string, TestingStuffInternal>();
        }

        // A5a: ConcurrentDictionary so the polling thread (reads in GetTaskStatus, AreAllDatabasePoliciesCompliant,
        // FinishBackup, DelayAsync, etc.) does not race with test-thread writes from RavenTestBase.Backup helpers.
        internal ConcurrentDictionary<string, TestingStuffInternal> DatabaseTestingStuffInternals { get; set; }
    }

    internal class TestingStuffInternal
    {
        internal Action OnMissingResponsibleNode;
        internal bool SimulateActiveByOtherNodeStatus_Reschedule;
        internal bool SimulateActiveByOtherNodeStatus_UpdateConfigurations;
        internal bool SimulateActiveByCurrentNode_UpdateConfigurations;
        internal bool SimulateDisableNodeStatus_UpdateConfigurations;
        internal bool SimulateFailedBackup;
        internal bool BackupStatusFromMemoryOnly;

        internal TaskCompletionSource<object> OnBackupTaskRunHoldBackupExecution;

        internal Action AfterBackupBatchCompleted;

        internal ManualResetEvent HoldBackupFromFinishing;

        // A5d: per-database test hooks. Previously these were server-wide properties on TestingStuff
        // (one delegate fired for every backup on every database on the server). Moved here so tests
        // can attach hooks scoped to a single database and other databases sharing the server are
        // unaffected.
        internal Action<IDatabaseBackupPolicy, string, DatabaseBackupState> OnDatabasePolicyViolation;
        internal Action<IServerBackupPolicy, string> OnServerPolicyViolation;
        internal Action<DatabaseBackupState> OnBeforeBackupStarted;
        internal Action<DatabaseBackupState> OnAfterBackupCompleted;
    }

}
