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

public class ServerBackupRunner : IDisposable
{

    public const int MaxDecisionLogSize = 1024;

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

    public void EnsureDatabaseRegistered(string databaseName)
    {
        BackupsPerDatabasePerTaskId.GetOrAdd(databaseName, static _ => new ConcurrentDictionary<long, DatabaseBackupState>());
    }

    private void RegisterNewBackup(DatabaseBackupState backupState)
    {
        var backupsPerDatabasePerTaskId = BackupsPerDatabasePerTaskId.GetOrAdd(backupState.DatabaseName, static _ => new ConcurrentDictionary<long, DatabaseBackupState>());

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

    private void RunBackup(DatabaseBackupState backupState, DateTime startTimeInUtc, bool isFullBackup = false, long? operationId = null)
    {
        if (_forTestingPurposes != null &&
            _forTestingPurposes.DatabaseTestingStuffInternals.TryGetValue(backupState.DatabaseName, out var testingStuffInternal))
        {
            testingStuffInternal.OnBeforeBackupStarted?.Invoke(backupState);
        }

        if (backupState.Running.Raise() == false)
            return;

        _serverStore.ConcurrentBackupsCounter.StartBackup(backupState.OriginalDatabaseName, backupState.Configuration.Name, _logger);

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
                    backupToLocalFolder && BackupTask.DirectoryContainsBackupFiles(localBackupStatus.LocalBackup.BackupDirectory, IsFullBackupOrSnapshot) == false)
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

                try
                {
                    backupState.BackupStatus.Version++;
                    backupState.BackupStatus.Error = new Error { Exception = e.ToString(), At = DateTime.UtcNow };
                    backupState.BackupStatus.IsFull = isFullBackup;
                    backupState.BackupStatus.NodeTag = _serverStore.NodeTag;
                    if (isFullBackup)
                        backupState.BackupStatus.LastFullBackupInternal = startTimeInUtc;
                    else
                        backupState.BackupStatus.LastIncrementalBackupInternal = startTimeInUtc;

                    BackupUtils.SaveBackupStatus(backupState.BackupStatus, backupState.DatabaseName, _serverStore, _logger);

                    _serverStore.NotificationCenter.Add(AlertRaised.Create(
                        backupState.DatabaseName,
                        $"Periodic Backup task: '{backupState.Configuration.Name}'",
                        $"Failed to start the backup task '{backupState.Configuration.Name}'. The next backup will be rescheduled.",
                        AlertReason.PeriodicBackup, NotificationSeverity.Error, details: new ExceptionDetails(e)));
                }
                catch (Exception reportError)
                {
                    if (_logger.IsErrorEnabled)
                        _logger.Error($"Failed to report backup start-failure for {backupState}.", reportError);
                }

                FinishBackup(backupState, backupState.DatabaseName);
            }
        });

        void FinishBackup(DatabaseBackupState databaseBackupState, string databaseName)
        {
            if (_forTestingPurposes != null &&
                _forTestingPurposes.DatabaseTestingStuffInternals != null &&
                _forTestingPurposes.DatabaseTestingStuffInternals.TryGetValue(databaseName, out TestingStuffInternal value))
            {
                value.HoldBackupFromFinishing?.WaitOne();
            }

            try
            {
                databaseBackupState.NextBackup = databaseBackupState.Stale ? null : databaseBackupState.GetNextBackupDetails(databaseBackupState.BackupStatus, out _);
            }
            catch (Exception e)
            {
                databaseBackupState.NextBackup = null; // BackupTimePolicy will recompute on the next tick

                if (_logger.IsWarnEnabled)
                    _logger.Warn($"Failed to schedule the next backup for {databaseBackupState}.", e);
            }

            databaseBackupState.RunningTask = null;

            backupState.Running.Lower();
            _serverStore.ConcurrentBackupsCounter.FinishBackup(databaseBackupState.OriginalDatabaseName, databaseBackupState.Configuration.Name, databaseBackupState.RunningBackupStatus, null, _logger);
            databaseBackupState.RunningBackupStatus = null;

            databaseBackupState.RunningCancel = null;
        }
    }

    public long StartBackupTask(string databaseName, long taskId, bool isFullBackup, long operationId, DateTime? startTimeUtc = null)
    {
        DatabaseBackupState databaseBackupState = GetDatabaseStateByTaskId(databaseName, taskId);
        if (databaseBackupState == null)
        {
            throw new InvalidOperationException($"Backup task id: {taskId} doesn't exist");
        }
        var runningTask = databaseBackupState.RunningTask;
        if (databaseBackupState.Running && runningTask != null)
        {
            throw new BackupAlreadyRunningException(
                $"Could not start backup task '{databaseBackupState.Configuration.TaskId}' because there is already a running backup under operation id '{runningTask.Id}'")
            {
                OperationId = runningTask.Id,
                NodeTag = _serverStore.NodeTag
            };
        }
        RunBackup(databaseBackupState, startTimeUtc ?? SystemTime.UtcNow, isFullBackup, operationId);
        return operationId;
    }

    private Task<IOperationResult> StartBackupThread(DocumentDatabase database, DatabaseBackupState backupState, BackupTask backupTask, TaskCompletionSource<IOperationResult> tcs, Action<IOperationProgress> onProgress)
    {
        var threadName = $"Backup task {backupState.Configuration.Name} for database '{database.Name}'";
        PoolOfThreads.GlobalRavenThreadPool.LongRunning(_ => RunBackupThread(database, backupState, backupTask, threadName, tcs, onProgress), null, ThreadNames.ForBackupTask(threadName,
            database.Name, backupState.Configuration.Name));
        return tcs.Task;
    }

    private void RunBackupThread(DocumentDatabase database, DatabaseBackupState backupState, BackupTask backupTask, string threadName, TaskCompletionSource<IOperationResult> tcs, Action<IOperationProgress> onProgress)
    {
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
                BackupResult backupResult = backupTask.RunPeriodicBackup(onProgress, backupState, tcs.Task, ref runningBackupStatus);

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

    public async Task DelayAsync(string databaseName, long operationId, DateTime delayUntil, X509Certificate2 clientCert, CancellationToken token)
    {
        if (BackupsPerDatabasePerTaskId.TryGetValue(databaseName, out var backupsPerDatabasePerTaskId) == false)
            throw new InvalidOperationException($"Fail to delay backup task with operationId '{operationId}',  the operation with that number isn't registered");

        foreach (var databaseBackupState in backupsPerDatabasePerTaskId)
        {
            var runningTask = databaseBackupState.Value.RunningTask;
            if (runningTask == null || runningTask.Id != operationId)
                continue;

            var nextBackup = databaseBackupState.Value.GetNextBackupDetails(databaseBackupState.Value.BackupStatus, out string _);

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
            databaseBackupState.Value.NextBackup = databaseBackupState.Value.GetNextBackupDetails(databaseBackupState.Value.BackupStatus, out string _);

            if (_forTestingPurposes != null &&
                _forTestingPurposes.DatabaseTestingStuffInternals != null &&
                _forTestingPurposes.DatabaseTestingStuffInternals.TryGetValue(databaseName, out TestingStuffInternal testingStuffInternal))
            {
                testingStuffInternal.OnBackupTaskRunHoldBackupExecution?.SetResult(null);
            }

            var database = await _serverStore.DatabasesLandlord.TryGetOrCreateDatabase(databaseBackupState.Value.DatabaseName).DatabaseTask;

            await database.Operations.KillOperationAsync(operationId, token);

            try
            {
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
            _forTestingPurposes.DatabaseTestingStuffInternals.TryGetValue(databaseName, out TestingStuffInternal testingStuffInternal))
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

    public void HandleDatabaseValueChanged(string type, object changeState, string databaseName)
    {
        switch (type)
        {
            case nameof(UpdateResponsibleNodeForTasksCommand):
                using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (context.OpenReadTransaction())
                using (var rawRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, databaseName))
                {
                    if (rawRecord != null)
                        HandleDatabaseRecordChange(rawRecord);
                }
                break;

            case nameof(DelayBackupCommand):
                var state = (DelayBackupCommand.DelayBackupCommandState)changeState;

                var databaseBackupState = GetDatabaseStateByTaskId(databaseName, state.TaskId);
                if (databaseBackupState == null)
                    throw new InvalidOperationException($"Backup task id: {state.TaskId} doesn't exist");

                databaseBackupState.BackupStatus ??= new PeriodicBackupStatus { TaskId = state.TaskId };
                databaseBackupState.BackupStatus.DelayUntil = state.DelayUntil;
                databaseBackupState.BackupStatus.OriginalBackupTime = state.OriginalBackupTime;
                break;
        }
    }

    public void HandleDatabaseRecordChange(RawDatabaseRecord databaseRecord)
    {
        var databaseName = databaseRecord.DatabaseName;

        if (BackupsPerDatabasePerTaskId.TryGetValue(databaseName, out ConcurrentDictionary<long, DatabaseBackupState> backupStates) == false)
            return;

        var configurations = databaseRecord.PeriodicBackups;

        if (configurations == null || configurations.Count == 0)
        {
            foreach (var kvp in backupStates.ForceEnumerateInThreadSafeManner())
            {
                kvp.Value.Stale.Raise();
                kvp.Value.CancelRunningBackup(CancelReasonTaskDeleted);
            }

            backupStates.Clear();
            return;
        }

        var allBackupTaskIds = new List<long>(configurations.Count);
        foreach (var configuration in configurations)
        {
            allBackupTaskIds.Add(configuration.TaskId);
            ApplyBackupConfiguration(databaseName, configuration);
        }

        foreach (var deletedBackupTaskId in backupStates.Keys.Except(allBackupTaskIds))
        {
            if (backupStates.TryRemove(deletedBackupTaskId, out var backupState) == false)
                continue;

            backupState.Stale.Raise();
            backupState.CancelRunningBackup(CancelReasonTaskDeleted);
        }
    }

    private void ApplyBackupConfiguration(string databaseName, PeriodicBackupConfiguration configuration)
    {
        var taskState = GetTaskStatus(configuration, databaseName, out _);
        if (_forTestingPurposes != null &&
            _forTestingPurposes.DatabaseTestingStuffInternals != null &&
            _forTestingPurposes.DatabaseTestingStuffInternals.TryGetValue(databaseName, out TestingStuffInternal testingStuffInternal))
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

        UpdatePeriodicBackup(databaseName, configuration.TaskId, configuration, taskState);
    }

    private void UpdatePeriodicBackup(string databaseName, long taskId,
        PeriodicBackupConfiguration newConfiguration,
        TaskStatus taskState)
    {
        Debug.Assert(taskId == newConfiguration.TaskId);

        var name = databaseName;
        var existingBackupState = GetDatabaseStateByTaskId(name, taskId);

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
                return;

            case TaskStatus.ActiveByCurrentNode:
                if (existingBackupState.Stale)
                {
                    existingBackupState.Stale.Lower();
                    _backupQueue.Enqueue(existingBackupState);
                }

                if (existingBackupState.RunningTask != null)
                {
                    if (_logger.IsDebugEnabled)
                        _logger.Debug($"Backup task '{taskId}' state is '{taskState}', and currently are being executed since '{existingBackupState.StartTimeInUtc}'.");
                    return;
                }

                if (previousConfiguration.HasBackupFrequencyChanged(newConfiguration) == false)
                {
                    if (_logger.IsDebugEnabled)
                        _logger.Debug($"Backup task '{taskId}' state is '{taskState}', the task doesn't have frequency changes and has scheduled backup, will continue to execute by the current node '{_serverStore.NodeTag}'.");

                    return;
                }

                if (_logger.IsDebugEnabled)
                    _logger.Debug($"Backup task '{taskId}' state is '{taskState}', the task has frequency changes or doesn't have scheduled backup, the timer will be rearranged and the task will be executed by current node '{_serverStore.NodeTag}'.");


                var localBackupStatus = existingBackupState.GetMostUpdatedLocalBackupStatus(taskId, inMemoryBackupStatus: null, name);
                existingBackupState.NextBackup = existingBackupState.GetNextBackupDetails(localBackupStatus, out string _);

                return;

            default:
                throw new ArgumentOutOfRangeException(nameof(taskState), taskState, null);
        }
    }

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
            if (_forTestingPurposes != null &&
                _forTestingPurposes.DatabaseTestingStuffInternals != null &&
                _forTestingPurposes.DatabaseTestingStuffInternals.TryGetValue(databaseName, out TestingStuffInternal testingStuffInternal))
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

    public DatabaseBackupState GetDatabaseStateByTaskId(string databaseName, long taskId)
    {
        if (BackupsPerDatabasePerTaskId.TryGetValue(databaseName, out var backupsPerDatabasePerTaskId) &&
             backupsPerDatabasePerTaskId.TryGetValue(taskId, out var databaseBackupState))
        {
            return databaseBackupState;
        }

        return null;
    }

    public List<DatabaseBackupState> GetDatabaseBackups(string databaseName)
    {
        return BackupsPerDatabasePerTaskId.TryGetValue(databaseName, out var backupsPerDatabasePerTaskId) ?
            backupsPerDatabasePerTaskId.Values.ToList() : [];
    }

    public NextBackup GetNextBackupDetails(long taskId, string databaseName, PeriodicBackupStatus backupStatus, out string tag)
    {
        tag = null;

        var state = GetDatabaseStateByTaskId(databaseName, taskId);
        return state == null ? null : state.GetNextBackupDetails(backupStatus, out tag);
    }

    public enum TaskStatus
    {
        Disabled,
        ActiveByCurrentNode,
        ActiveByOtherNode,
        MissingResponsibleNode
    }

    public BackupInfo GetBackupInfo(string databaseName)
    {
        using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            return GetBackupInfo(context, databaseName);
        }
    }

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
                        _forTestingPurposes.DatabaseTestingStuffInternals != null &&
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
        var thread = _thread;
        if (thread != null && thread != PoolOfThreads.LongRunningWork.Current)
            thread.Join(int.MaxValue);

    }

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

        _forTestingPurposes?.DatabaseTestingStuffInternals?.TryRemove(databaseName, out _);
    }

    public RunningBackup OnGoingBackup(string databaseName, long taskId)
    {
        var state = GetDatabaseStateByTaskId(databaseName, taskId);
        if (state == null)
            return null;

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


    public class TestingStuff
    {
        public TestingStuff()
        {
            DatabaseTestingStuffInternals = new ConcurrentDictionary<string, TestingStuffInternal>();
        }

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
        internal Action<IDatabaseBackupPolicy, string, DatabaseBackupState> OnDatabasePolicyViolation;
        internal Action<IServerBackupPolicy, string> OnServerPolicyViolation;
        internal Action<DatabaseBackupState> OnBeforeBackupStarted;
        internal Action<DatabaseBackupState> OnAfterBackupCompleted;
    }

}
