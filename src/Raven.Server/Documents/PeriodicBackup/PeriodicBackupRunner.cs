using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Extensions;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Sharding;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.Server.Utils;
using Sparrow.Utils;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.PeriodicBackup
{
    public sealed class PeriodicBackupRunner : ITombstoneAware, IDisposable
    {
        private readonly Logger _logger;

        private readonly DocumentDatabase _database;
        private readonly ServerStore _serverStore;
        private readonly CancellationTokenSource _cancellationToken;
        private readonly PathSetting _tempBackupPath;
        private readonly string _originalDatabaseName;

        private readonly ConcurrentDictionary<long, PeriodicBackup> _periodicBackups
            = new ConcurrentDictionary<long, PeriodicBackup>();

        private readonly ConcurrentSet<Task> _inactiveRunningPeriodicBackupsTasks = new ConcurrentSet<Task>();

        private bool _disposed;
        private readonly DateTime? _databaseWakeUpTimeUtc;

        // interval can be 2^32-2 milliseconds at most
        // this is the maximum interval acceptable in .Net's threading timer
        public readonly TimeSpan MaxTimerTimeout = TimeSpan.FromMilliseconds(Math.Pow(2, 32) - 2);

        public ICollection<PeriodicBackup> PeriodicBackups => _periodicBackups.Values;

        public PeriodicBackupRunner(DocumentDatabase database, ServerStore serverStore, DateTime? wakeup = null)
        {
            _database = database;
            _serverStore = serverStore;
            _logger = LoggingSource.Instance.GetLogger<PeriodicBackupRunner>(_database.Name);
            _cancellationToken = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
            _tempBackupPath = BackupUtils.GetBackupTempPath(_database.Configuration, "PeriodicBackupTemp", out _);
            _originalDatabaseName = database is ShardedDocumentDatabase sdd ? sdd.ShardedDatabaseName : database.Name;

            // we pass wakeup-1 to ensure the backup will run right after DB woke up on wakeup time, and not on the next occurrence.
            // relevant only if it's the first backup after waking up
            _databaseWakeUpTimeUtc = wakeup?.AddMinutes(-1);

            _database.TombstoneCleaner.Subscribe(this);
            IOExtensions.DeleteDirectory(_tempBackupPath.FullPath);
            IOExtensions.CreateDirectory(_tempBackupPath.FullPath);
        }

        public NextBackup GetNextBackupDetails(PeriodicBackupConfiguration configuration, PeriodicBackupStatus backupStatus, out string responsibleNodeTag)
        {
            var taskStatus = GetTaskStatus(configuration, out responsibleNodeTag, disableLog: true);
            return taskStatus == TaskStatus.Disabled ? null : GetNextBackupDetails(configuration, backupStatus, responsibleNodeTag, skipErrorLog: true);
        }

        private NextBackup GetNextBackupDetails(
            PeriodicBackupConfiguration configuration,
            PeriodicBackupStatus backupStatus,
            string responsibleNodeTag,
            bool skipErrorLog = false)
        {
            return BackupUtils.GetNextBackupDetails(new BackupUtils.NextBackupDetailsParameters
            {
                OnParsingError = skipErrorLog ? null : OnParsingError,
                Configuration = configuration,
                BackupStatus = backupStatus,
                ResponsibleNodeTag = responsibleNodeTag,
                DatabaseWakeUpTimeUtc = _databaseWakeUpTimeUtc,
                NodeTag = _serverStore.NodeTag,
                OnMissingNextBackupInfo = OnMissingNextBackupInfo
            });
        }

        private static bool IsFullBackupOrSnapshot(string filePath)
        {
            var extension = Path.GetExtension(filePath);
            return Constants.Documents.PeriodicBackup.FullBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                   Constants.Documents.PeriodicBackup.SnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                   Constants.Documents.PeriodicBackup.EncryptedFullBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                   Constants.Documents.PeriodicBackup.EncryptedSnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);
        }

        internal void TimerCallback(object backupTaskDetails)
        {
            try
            {
                var backupDetails = (NextBackup)backupTaskDetails;

                if (_cancellationToken.IsCancellationRequested)
                {
                    if (_logger.IsOperationsEnabled)
                    {
                        var type = backupDetails.IsFull ? "full" : "incremental";
                        _logger.Operations($"Canceling the {type} backup task '{backupDetails.TaskId}' after the {nameof(TimerCallback)}.");
                    }

                    return;
                }

                if (ShouldRunBackupAfterTimerCallbackAndRescheduleIfNeeded(backupDetails, out PeriodicBackup periodicBackup) == false)
                    return;

                StartBackupTaskAndRescheduleIfNeeded(periodicBackup, backupDetails);
            }
            catch (Exception e)
            {
                _logger.Operations("Error during timer callback", e);
            }
        }

        internal void LongPeriodTimerCallback(object backupTaskDetails)
        {
            try
            {
                var backupDetails = (NextBackup)backupTaskDetails;

                if (_cancellationToken.IsCancellationRequested)
                {
                    if (_logger.IsOperationsEnabled)
                    {
                        var type = backupDetails.IsFull ? "full" : "incremental";
                        _logger.Operations($"Canceling the {type} backup task '{backupDetails.TaskId}' after the {nameof(LongPeriodTimerCallback)}.");
                    }
                    return;
                }

                if (ShouldRunBackupAfterTimerCallbackAndRescheduleIfNeeded(backupDetails, out PeriodicBackup periodicBackup) == false)
                    return;

                var remainingInterval = backupDetails.TimeSpan - MaxTimerTimeout;
                if (remainingInterval.TotalMilliseconds <= 0)
                {
                    StartBackupTaskAndRescheduleIfNeeded(periodicBackup, backupDetails);
                    return;
                }

                periodicBackup.UpdateTimer(GetNextBackupDetails(periodicBackup.Configuration, periodicBackup.BackupStatus, _serverStore.NodeTag), lockTaken: false);
            }
            catch (Exception e)
            {
                _logger.Operations("Error during long timer callback", e);
            }
        }

        private void StartBackupTaskAndRescheduleIfNeeded(PeriodicBackup periodicBackup, NextBackup currentBackup)
        {
            try
            {
                CreateBackupTask(periodicBackup, currentBackup.IsFull, currentBackup.DateTime);
            }
            catch (BackupDelayException e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Backup task will be retried in {(int)e.DelayPeriod.TotalSeconds} seconds, Reason: {e.Message}");

                // we'll retry in one minute
                var backupTaskDetails = new NextBackup
                {
                    IsFull = currentBackup.IsFull,
                    TaskId = periodicBackup.Configuration.TaskId,
                    DateTime = DateTime.UtcNow.Add(e.DelayPeriod),
                    TimeSpan = e.DelayPeriod
                };

                periodicBackup.UpdateTimer(backupTaskDetails, lockTaken: false);
            }
        }

        public string WhoseTaskIsIt(long taskId)
        {
            if (_periodicBackups.TryGetValue(taskId, out var periodicBackup) == false)
            {
                throw new InvalidOperationException($"Backup task id: {taskId} doesn't exist");
            }

            if (periodicBackup.Configuration.Disabled)
            {
                throw new InvalidOperationException($"Backup task id: {taskId} is disabled");
            }

            if (periodicBackup.Configuration.HasBackup() == false)
            {
                throw new InvalidOperationException($"All backup destinations are disabled for backup task id: {taskId}");
            }

            return BackupUtils.GetResponsibleNodeTag(_serverStore, _database.Name, periodicBackup.Configuration.TaskId);
        }

        public long StartBackupTask(long taskId, bool isFullBackup, long? operationId = null, DateTime? startTimeUtc = null)
        {
            if (_periodicBackups.TryGetValue(taskId, out var periodicBackup) == false)
            {
                throw new InvalidOperationException($"Backup task id: {taskId} doesn't exist");
            }

            return CreateBackupTask(periodicBackup, isFullBackup, startTimeUtc ?? SystemTime.UtcNow, operationId);
        }

        public async Task DelayAsync(long taskId, DateTime delayUntil, X509Certificate2 clientCert, CancellationToken token)
        {
            foreach (var periodicBackup in _periodicBackups)
            {
                var runningTask = periodicBackup.Value.RunningTask;
                if (runningTask == null || runningTask.Id != taskId)
                    continue;

                var nextBackup = GetNextBackupDetails(
                    periodicBackup.Value.Configuration, 
                    periodicBackup.Value.BackupStatus,
                    periodicBackup.Value.Configuration.MentorNode, 
                    skipErrorLog: true);

                var originalBackupTime = delayUntil > nextBackup.DateTime 
                    ? nextBackup.DateTime 
                    : periodicBackup.Value.StartTimeInUtc;

                var command = new DelayBackupCommand(_database.Name, RaftIdGenerator.NewId())
                {
                    TaskId = periodicBackup.Key,
                    DelayUntil = delayUntil,
                    OriginalBackupTime = originalBackupTime
                };

                try
                {
                    (long index, _) = await _database.ServerStore.SendToLeaderAsync(command);
                    await _database.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, index);
                }
                catch (Exception e)
                {
                    if (_logger.IsOperationsEnabled)
                    {
                        var msg =
                            $"Fail to delay backup task with task id '{taskId}' cluster-wide, the task was delayed until '{delayUntil}' UTC only on the current node.";
                        
                        _logger.Operations(msg, e);
                    }
                }

                periodicBackup.Value.BackupStatus.DelayUntil = delayUntil;
                periodicBackup.Value.BackupStatus.OriginalBackupTime = originalBackupTime;

                _forTestingPurposes?.OnBackupTaskRunHoldBackupExecution?.SetResult(null);
                await _database.Operations.KillOperationAsync(taskId, token);
                
                try
                {
                    await runningTask.Task; // wait for the running task to complete
                }
                catch
                {
                    // task has ended, nothing we can do here
                }
                
                return;
            }

            throw new InvalidOperationException($"Fail to delay backup task with task id '{taskId}', the operation with that number isn't registered");
        }

        public IdleDatabaseActivity GetNextIdleDatabaseActivity(string databaseName)
        {
            if (_periodicBackups.IsEmpty)
                return null;

            long lastEtag;

            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                lastEtag = _database.DocumentsStorage.ReadLastEtag(tx.InnerTransaction);
            }

            return BackupUtils.GetEarliestIdleDatabaseActivity(new BackupUtils.EarliestIdleDatabaseActivityParameters
            {
                DatabaseName = databaseName,
                DatabaseWakeUpTimeUtc = _databaseWakeUpTimeUtc,
                LastEtag = lastEtag,
                Logger = _logger,
                NotificationCenter = _database.NotificationCenter,
                OnParsingError = OnParsingError,
                OnMissingNextBackupInfo = OnMissingNextBackupInfo,
                ServerStore = _serverStore,
                IsIdle = false
            });
        }

        private long CreateBackupTask(PeriodicBackup periodicBackup, bool isFullBackup, DateTime startTimeInUtc, long? operationId = null)
        {
            using (periodicBackup.UpdateBackupTask())
            {
                if (periodicBackup.Disposed)
                    throw new InvalidOperationException("Backup task was already disposed");

                if (_database.DisableOngoingTasks)
                    throw new InvalidOperationException("Backup task is disabled via marker file");

                var runningTask = periodicBackup.RunningTask;
                if (runningTask != null)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Could not start backup task '{periodicBackup.Configuration.TaskId}' because there is already a running backup '{runningTask.Id}'");

                    return runningTask.Id;
                }

                BackupUtils.CheckServerHealthBeforeBackup(_serverStore, periodicBackup.Configuration.Name);
                _serverStore.ConcurrentBackupsCounter.StartBackup(_originalDatabaseName, periodicBackup.Configuration.Name, _logger);

                var tcs = new TaskCompletionSource<IOperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);

                try
                {
                    var backupStatus = periodicBackup.BackupStatus = GetBackupStatus(periodicBackup.Configuration.TaskId, periodicBackup.BackupStatus);
                    var backupToLocalFolder = BackupConfiguration.CanBackupUsing(periodicBackup.Configuration.LocalSettings);

                    // check if we need to do a new full backup
                    if (backupStatus.LastFullBackup == null || // no full backup was previously performed
                        backupStatus.NodeTag != _serverStore.NodeTag || // last backup was performed by a different node
                        backupStatus.BackupType != periodicBackup.Configuration.BackupType || // backup type has changed
                        backupStatus.LastEtag == null || // last document etag wasn't updated
                        backupToLocalFolder && BackupTask.DirectoryContainsBackupFiles(backupStatus.LocalBackup.BackupDirectory, IsFullBackupOrSnapshot) == false)
                    // the local folder already includes a full backup or snapshot
                    {
                        isFullBackup = true;
                    }

                    operationId ??= _database.Operations.GetNextOperationId();
                    var backupTypeText = GetBackupTypeText(isFullBackup, periodicBackup.Configuration.BackupType);

                    periodicBackup.StartTimeInUtc = startTimeInUtc;

                    var backupParameters = new BackupParameters
                    {
                        RetentionPolicy = periodicBackup.Configuration.RetentionPolicy,
                        StartTimeUtc = periodicBackup.StartTimeInUtc,
                        BackupStatus = periodicBackup.BackupStatus,
                        IsOneTimeBackup = false,
                        IsFullBackup = isFullBackup,
                        BackupToLocalFolder = backupToLocalFolder,
                        TempBackupPath = _tempBackupPath,
                        OperationId = operationId.Value,
                        Name = periodicBackup.Configuration.Name
                    };

                    var backupTask = BackupUtils.GetBackupTask(_database, backupParameters, periodicBackup.Configuration, token: null, _logger, _forTestingPurposes);
                    periodicBackup.CancelToken = backupTask.TaskCancelToken;

                    periodicBackup.RunningTask = new PeriodicBackup.RunningBackupTask
                    {
                        Id = operationId.Value,
                        Task = tcs.Task
                    };

                    var task = _database.Operations.AddLocalOperation(
                        operationId.Value,
                        OperationType.DatabaseBackup,
                        $"{backupTypeText} backup task: '{periodicBackup.Configuration.Name}'. Database: '{_database.Name}'",
                        detailedDescription: null,
                        taskFactory: onProgress => StartBackupThread(periodicBackup, backupTask, tcs, onProgress),
                        token: backupTask.TaskCancelToken);

                    task.ContinueWith(_ => backupTask.TaskCancelToken.Dispose());

                    return operationId.Value;
                }
                catch (Exception e)
                {
                    // we failed to START the backup, need to update the status anyway
                    // in order to reschedule the next full/incremental backup
                    tcs.TrySetException(e);
                    periodicBackup.BackupStatus.Version++;
                    periodicBackup.BackupStatus.Error = new Error
                    {
                        Exception = e.ToString(),
                        At = DateTime.UtcNow
                    };

                    if (isFullBackup)
                        periodicBackup.BackupStatus.LastFullBackupInternal = startTimeInUtc;
                    else
                        periodicBackup.BackupStatus.LastIncrementalBackupInternal = startTimeInUtc;
                    
                    BackupUtils.SaveBackupStatus(periodicBackup.BackupStatus, _database.Name, _database.ServerStore, _logger, operationCancelToken: periodicBackup.CancelToken);

                    var message = $"Failed to start the backup task: '{periodicBackup.Configuration.Name}'";
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations(message, e);

                    ScheduleNextBackup(periodicBackup, elapsed: null, lockTaken: true);

                    _database.NotificationCenter.Add(AlertRaised.Create(
                        _database.Name,
                        message,
                        "The next backup will be rescheduled",
                        AlertType.PeriodicBackup,
                        NotificationSeverity.Error,
                        details: new ExceptionDetails(e)));

                    throw;
                }
            }
        }

        private Task<IOperationResult> StartBackupThread(PeriodicBackup periodicBackup, BackupTask backupTask, TaskCompletionSource<IOperationResult> tcs, Action<IOperationProgress> onProgress)
        {
            var threadName = $"Backup task {periodicBackup.Configuration.Name} for database '{_database.Name}'";
            PoolOfThreads.GlobalRavenThreadPool.LongRunning(x => RunBackupThread(periodicBackup, backupTask, threadName, tcs, onProgress), null, ThreadNames.ForBackupTask(threadName,
                _database.Name, periodicBackup.Configuration.Name));
            return tcs.Task;
        }

        private void RunBackupThread(PeriodicBackup periodicBackup, BackupTask backupTask, string threadName, TaskCompletionSource<IOperationResult> tcs, Action<IOperationProgress> onProgress)
        {
            BackupResult backupResult = null;
            var runningBackupStatus = new PeriodicBackupStatus
            {
                TaskId = periodicBackup.Configuration.TaskId,
                BackupType = periodicBackup.Configuration.BackupType,
                LastEtag = periodicBackup.BackupStatus.LastEtag,
                LastRaftIndex = periodicBackup.BackupStatus.LastRaftIndex,
                LastFullBackup = periodicBackup.BackupStatus.LastFullBackup,
                LastIncrementalBackup = periodicBackup.BackupStatus.LastIncrementalBackup,
                LastFullBackupInternal = periodicBackup.BackupStatus.LastFullBackupInternal,
                LastIncrementalBackupInternal = periodicBackup.BackupStatus.LastIncrementalBackupInternal,
                IsFull = backupTask._isFullBackup,
                LocalBackup = periodicBackup.BackupStatus.LocalBackup,
                LastOperationId = periodicBackup.BackupStatus.LastOperationId,
                FolderName = periodicBackup.BackupStatus.FolderName,
                LastDatabaseChangeVector = periodicBackup.BackupStatus.LastDatabaseChangeVector
            };

            periodicBackup.RunningBackupStatus = runningBackupStatus;

            try
            {
                ThreadHelper.TrySetThreadPriority(ThreadPriority.BelowNormal, threadName, _logger);
                NativeMemory.EnsureRegistered();

                using (_database.PreventFromUnloadingByIdleOperations())
                {
                    backupResult = (BackupResult)backupTask.RunPeriodicBackup(onProgress, ref runningBackupStatus);
                    tcs.SetResult(backupResult);
                }
            }
            catch (Exception e) when (e.ExtractSingleInnerException() is OperationCanceledException oce)
            {
                if (_periodicBackups.TryGetValue(periodicBackup.BackupStatus.TaskId, out PeriodicBackup inMemoryBackupStatus))
                {
                    runningBackupStatus.DelayUntil = inMemoryBackupStatus.BackupStatus.DelayUntil;
                    runningBackupStatus.OriginalBackupTime = inMemoryBackupStatus.BackupStatus.OriginalBackupTime;
                }

                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Canceled the backup thread: '{periodicBackup.Configuration.Name}'", oce);

                tcs.SetCanceled();
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Failed to run the backup thread: '{periodicBackup.Configuration.Name}'", e);

                tcs.SetException(e);
            }
            finally
            {
                periodicBackup.BackupStatus = runningBackupStatus;
                ScheduleNextBackup(periodicBackup, backupResult?.Elapsed, lockTaken: false);
            }
        }

        private void ScheduleNextBackup(PeriodicBackup periodicBackup, TimeSpan? elapsed, bool lockTaken)
        {
            try
            {
                _serverStore.ConcurrentBackupsCounter.FinishBackup(_originalDatabaseName, periodicBackup.Configuration.Name, periodicBackup.RunningBackupStatus, elapsed, _logger);

                periodicBackup.RunningTask = null;
                periodicBackup.CancelToken = null;
                periodicBackup.RunningBackupStatus = null;

                if (periodicBackup.HasScheduledBackup() && _cancellationToken.IsCancellationRequested == false)
                    periodicBackup.UpdateTimer(GetNextBackupDetails(periodicBackup.Configuration, periodicBackup.BackupStatus, _serverStore.NodeTag), lockTaken, discardIfDisabled: true);
            }
            catch (Exception e)
            {
                var message = $"Failed to schedule next backup for task: '{periodicBackup.Configuration.Name}'";
                if (_logger.IsOperationsEnabled)
                    _logger.Operations(message, e);

                _database.NotificationCenter.Add(AlertRaised.Create(
                    _database.Name,
                    "Couldn't schedule next backup",
                    message,
                    AlertType.PeriodicBackup,
                    NotificationSeverity.Warning,
                    details: new ExceptionDetails(e)));
            }
        }

        private static string GetBackupTypeText(bool isFullBackup, BackupType backupType)
        {
            if (backupType == BackupType.Backup)
            {
                return isFullBackup ? "Full" : "Incremental";
            }

            return isFullBackup ? "Snapshot" : "Incremental Snapshot";
        }

        private bool ShouldRunBackupAfterTimerCallbackAndRescheduleIfNeeded(NextBackup backupInfo, out PeriodicBackup periodicBackup)
        {
            if (_periodicBackups.TryGetValue(backupInfo.TaskId, out periodicBackup) == false)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Backup {backupInfo.TaskId}, doesn't exist anymore");

                // periodic backup doesn't exist anymore
                return false;
            }

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var rawRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, _database.Name))
            {
                if (rawRecord == null)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Couldn't run backup task '{backupInfo.TaskId}' because database '{_database.Name}' record is null.");

                    return false;
                }
            }

            var taskStatus = GetTaskStatus(periodicBackup.Configuration, out _);
            if (_forTestingPurposes != null)
            {
                if (_forTestingPurposes.SimulateActiveByOtherNodeStatus_Reschedule)
                {
                    taskStatus = TaskStatus.ActiveByOtherNode;
                }
            }

            string msg;
            switch (taskStatus)
            {
                case TaskStatus.ActiveByCurrentNode:
                    msg = $"Backup {backupInfo.TaskId}, current status is {taskStatus}, the backup will be executed on current node.";
                    break;

                case TaskStatus.MissingResponsibleNode:
                    msg = $"Backup {backupInfo.TaskId}, current status is {taskStatus}, the responsible node wasn't determined yet.";
                    break;

                default:
                    msg = $"Backup {backupInfo.TaskId}, current status is {taskStatus}, the backup will be canceled on current node.";
                    periodicBackup.DisableFutureBackups();
                    break;
            }

            if (_logger.IsOperationsEnabled)
                _logger.Operations(msg);

            return taskStatus == TaskStatus.ActiveByCurrentNode;
        }

        public PeriodicBackupStatus GetBackupStatus(long taskId)
        {
            PeriodicBackupStatus inMemoryBackupStatus = null;
            if (_periodicBackups.TryGetValue(taskId, out PeriodicBackup periodicBackup))
                inMemoryBackupStatus = periodicBackup.BackupStatus;

            if (_forTestingPurposes != null && _forTestingPurposes.BackupStatusFromMemoryOnly)
                return inMemoryBackupStatus;
            
            return GetBackupStatus(taskId, inMemoryBackupStatus);
        }
        
        private PeriodicBackupStatus GetBackupStatus(long taskId, PeriodicBackupStatus inMemoryBackupStatus)
        {
            var backupStatus = GetBackupStatusFromCluster(_serverStore, _database.Name, taskId);
            return BackupUtils.ComparePeriodicBackupStatus(taskId, backupStatus, inMemoryBackupStatus);
        }

        private static PeriodicBackupStatus GetBackupStatusFromCluster(ServerStore serverStore, string databaseName, long taskId)
        {
            using (serverStore.Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                return BackupUtils.GetBackupStatusFromCluster(serverStore, context, databaseName, taskId);
            }
        }

        private long GetMinLastEtag()
        {
            var min = long.MaxValue;

            using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                var record = _serverStore.Cluster.ReadRawDatabaseRecord(context, _database.Name);
                foreach (var taskId in record.PeriodicBackupsTaskIds)
                {
                    var config = record.GetPeriodicBackupConfiguration(taskId);
                    if (config.IncrementalBackupFrequency == null)
                    {
                        // if there is no status for this, we don't need to take into account tombstones
                        continue; // if the backup is always full, we don't need to take into account the tombstones, since we never back them up.
                    }
                    var status = BackupUtils.GetBackupStatusFromCluster(_serverStore, context, _database.Name, taskId);
                    if (status == null)
                    {
                        // if there is no status for this, we don't need to take into account tombstones
                        return 0; // cannot delete the tombstones until we've done a full backup
                    }
                    var etag = ChangeVectorUtils.GetEtagById(status.LastDatabaseChangeVector, _database.DbBase64Id);
                    min = Math.Min(etag, min);
                }

                return min;
            }
        }

        public void UpdateConfigurations(List<PeriodicBackupConfiguration> configurations)
        {
            if (_disposed)
                return;

            if (configurations == null)
            {
                foreach (var periodicBackup in _periodicBackups)
                {
                    periodicBackup.Value.Dispose();
                }
                _periodicBackups.Clear();
                return;
            }

            var allBackupTaskIds = new List<long>();
            foreach (var periodicBackupConfiguration in configurations)
            {
                var newBackupTaskId = periodicBackupConfiguration.TaskId;
                allBackupTaskIds.Add(newBackupTaskId);

                var taskState = GetTaskStatus(periodicBackupConfiguration, out _);
                if (_forTestingPurposes != null)
                {
                    if (_forTestingPurposes.SimulateActiveByOtherNodeStatus_UpdateConfigurations)
                    {
                        taskState = TaskStatus.ActiveByOtherNode;
                    }
                    else if (_forTestingPurposes.SimulateDisableNodeStatus_UpdateConfigurations)
                    {
                        taskState = TaskStatus.Disabled;
                    }
                    else if (_forTestingPurposes.SimulateActiveByCurrentNode_UpdateConfigurations)
                    {
                        taskState = TaskStatus.ActiveByCurrentNode;
                    }
                }

                UpdatePeriodicBackup(newBackupTaskId, periodicBackupConfiguration, taskState);
            }

            var deletedBackupTaskIds = _periodicBackups.Keys.Except(allBackupTaskIds).ToList();
            foreach (var deletedBackupId in deletedBackupTaskIds)
            {
                if (_periodicBackups.TryRemove(deletedBackupId, out var deletedBackup) == false)
                    continue;

                // stopping any future backups
                // currently running backups will continue to run
                deletedBackup.Dispose();
            }
        }

        private void UpdatePeriodicBackup(long taskId,
            PeriodicBackupConfiguration newConfiguration,
            TaskStatus taskState)
        {
            Debug.Assert(taskId == newConfiguration.TaskId);

            if (_periodicBackups.TryGetValue(taskId, out var existingBackupState) == false)
            {
                var newPeriodicBackup = new PeriodicBackup(periodicBackupRunner: this, _inactiveRunningPeriodicBackupsTasks, _logger)
                {
                    Configuration = newConfiguration
                };

                var periodicBackup = _periodicBackups.GetOrAdd(taskId, newPeriodicBackup);
                if (periodicBackup != newPeriodicBackup)
                {
                    newPeriodicBackup.Dispose();
                }

                if (taskState == TaskStatus.ActiveByCurrentNode)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"New backup task '{taskId}' state is '{taskState}', will arrange a new backup timer.");

                    var backupStatus = GetBackupStatus(taskId, inMemoryBackupStatus: null);
                    periodicBackup.UpdateTimer(GetNextBackupDetails(newConfiguration, backupStatus, _serverStore.NodeTag), lockTaken: false);
                }

                return;
            }

            var previousConfiguration = existingBackupState.Configuration;
            existingBackupState.Configuration = newConfiguration;

            if (BackupHelper.BackupTypeChanged(previousConfiguration, newConfiguration))
            {
                // after deleting the periodic backup status we also clear the in-memory state backup status
                existingBackupState.BackupStatus = null;
            }

            switch (taskState)
            {
                case TaskStatus.Disabled:
                    existingBackupState.DisableFutureBackups();
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Backup task '{taskId}' state is '{taskState}', will cancel the backup for it.");

                    return;
                case TaskStatus.ActiveByOtherNode:
                    // the task is disabled or this node isn't responsible for the backup task
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Backup task '{taskId}' state is '{taskState}', will keep the timer for it.");

                    return;

                case TaskStatus.MissingResponsibleNode:
                    // the responsible node wasn't determined yet
                    return;

                case TaskStatus.ActiveByCurrentNode:
                    // a backup is already running, the next one will be re-scheduled by the backup task if needed
                    if (existingBackupState.RunningTask != null)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Backup task '{taskId}' state is '{taskState}', and currently are being executed since '{existingBackupState.StartTimeInUtc}'.");

                        return;
                    }

                    // backup frequency hasn't changed, and we have a scheduled backup
                    if (previousConfiguration.HasBackupFrequencyChanged(newConfiguration) == false && existingBackupState.HasScheduledBackup())
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Backup task '{taskId}' state is '{taskState}', the task doesn't have frequency changes and has scheduled backup, will continue to execute by the current node '{_database.ServerStore.NodeTag}'.");

                        return;
                    }

                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Backup task '{taskId}' state is '{taskState}', the task has frequency changes or doesn't have scheduled backup, the timer will be rearranged and the task will be executed by current node '{_database.ServerStore.NodeTag}'.");

                    var backupStatus = GetBackupStatus(taskId, inMemoryBackupStatus: null);
                    existingBackupState.UpdateTimer(GetNextBackupDetails(newConfiguration, backupStatus, _serverStore.NodeTag), lockTaken: false);
                    return;

                default:
                    throw new ArgumentOutOfRangeException(nameof(taskState), taskState, null);
            }
        }

        private enum TaskStatus
        {
            Disabled,
            ActiveByCurrentNode,
            ActiveByOtherNode,
            MissingResponsibleNode
        }

        private TaskStatus GetTaskStatus(PeriodicBackupConfiguration configuration, out string responsibleNodeTag, bool disableLog = false)
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
                    _database.NotificationCenter.Add(AlertRaised.Create(
                        _database.Name,
                        "Periodic Backup",
                        message,
                        AlertType.PeriodicBackup,
                        NotificationSeverity.Info));
                }

                responsibleNodeTag = null;
                return TaskStatus.Disabled;
            }

            responsibleNodeTag = BackupUtils.GetResponsibleNodeTag(_serverStore, _database.Name, configuration.TaskId);
            if (responsibleNodeTag == null)
            {
                // the responsible node wasn't set by the cluster observer yet
                _forTestingPurposes?.OnMissingResponsibleNode?.Invoke();

                return TaskStatus.MissingResponsibleNode;
            }

            if (responsibleNodeTag == _serverStore.NodeTag)
                return TaskStatus.ActiveByCurrentNode;

            if (disableLog == false && _logger.IsInfoEnabled)
                _logger.Info($"Backup job is skipped at {SystemTime.UtcNow}, because it is managed " +
                             $"by '{responsibleNodeTag}' node and not the current node ({_serverStore.NodeTag})");

            return TaskStatus.ActiveByOtherNode;
        }

        private void WaitForTaskCompletion(Task task)
        {
            try
            {
                task?.Wait();
            }
            catch (ObjectDisposedException)
            {
                // shutting down, probably
            }
            catch (AggregateException e) when (e.InnerException is OperationCanceledException)
            {
                // shutting down
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations("Error when disposing periodic backup runner task", e);
            }
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            lock (this)
            {
                if (_disposed)
                    return;

                _disposed = true;
                _database.TombstoneCleaner.Unsubscribe(this);

                using (_cancellationToken)
                {
                    _cancellationToken.Cancel();

                    foreach (var periodicBackup in _periodicBackups)
                    {
                        periodicBackup.Value.Dispose();
                    }

                    foreach (var inactiveTask in _inactiveRunningPeriodicBackupsTasks)
                    {
                        WaitForTaskCompletion(inactiveTask);
                    }
                }

                if (_tempBackupPath != null)
                    IOExtensions.DeleteDirectory(_tempBackupPath.FullPath);
            }
        }

        public BackupInfo GetBackupInfo()
        {
            using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                return BackupUtils.GetBackupInfo(
                    new BackupUtils.BackupInfoParameters
                    {
                        Context = context,
                        ServerStore = _serverStore,
                        PeriodicBackups = _periodicBackups.Values.ToList(),
                        DatabaseName = _database.Name
                    }
                );
            }
        }

        public BackupInfo GetBackupInfo(ClusterOperationContext context)
        {
            return BackupUtils.GetBackupInfo(
                new BackupUtils.BackupInfoParameters
                {
                    Context = context,
                    ServerStore = _serverStore,
                    PeriodicBackups = _periodicBackups.Values.ToList(),
                    DatabaseName = _database.Name
                }
            );
        }

        private void OnMissingNextBackupInfo(PeriodicBackupConfiguration configuration)
        {
            var message = "Couldn't schedule next backup " +
                          $"full backup frequency: {configuration.FullBackupFrequency}, " +
                          $"incremental backup frequency: {configuration.IncrementalBackupFrequency}";
            if (string.IsNullOrWhiteSpace(configuration.Name) == false)
                message += $", backup name: {configuration.Name}";

            _database.NotificationCenter.Add(AlertRaised.Create(
                _database.Name,
                "Couldn't schedule next backup, this shouldn't happen",
                message,
                AlertType.PeriodicBackup,
                NotificationSeverity.Warning));
        }

        private void OnParsingError(BackupUtils.OnParsingErrorParameters parameters)
        {
            var message = "Couldn't parse periodic backup " +
                          $"frequency {parameters.BackupFrequency}, task id: {parameters.Configuration.TaskId}";
            if (string.IsNullOrWhiteSpace(parameters.Configuration.Name) == false)
                message += $", backup name: {parameters.Configuration.Name}";

            message += $", error: {parameters.Exception.Message}";

            if (_logger.IsOperationsEnabled)
                _logger.Operations(message);

            _database.NotificationCenter.Add(AlertRaised.Create(
                _database.Name,
                "Backup frequency parsing error",
                message,
                AlertType.PeriodicBackup,
                NotificationSeverity.Error,
                details: new ExceptionDetails(parameters.Exception)));
        }

        public RunningBackup OnGoingBackup(long taskId)
        {
            if (_periodicBackups.TryGetValue(taskId, out var periodicBackup) == false)
                return null;

            var runningTask = periodicBackup.RunningTask;
            if (runningTask == null)
                return null;

            return new RunningBackup
            {
                StartTime = periodicBackup.StartTimeInUtc,
                IsFull = periodicBackup.RunningBackupStatus?.IsFull ?? false,
                RunningBackupTaskId = runningTask.Id
            };
        }

        public string TombstoneCleanerIdentifier => "Periodic Backup";

        public Dictionary<string, long> GetLastProcessedTombstonesPerCollection(ITombstoneAware.TombstoneType tombstoneType)
        {
            var minLastEtag = GetMinLastEtag();

            if (minLastEtag == long.MaxValue)
                return null;

            var result = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
            switch (tombstoneType)
            {
                case ITombstoneAware.TombstoneType.Documents:
                    result.Add(Constants.Documents.Collections.AllDocumentsCollection, minLastEtag);
                    break;
                case ITombstoneAware.TombstoneType.TimeSeries:
                    result.Add(Constants.TimeSeries.All, minLastEtag);
                    break;
                case ITombstoneAware.TombstoneType.Counters:
                    result.Add(Constants.Counters.All, minLastEtag);
                    break;
                default:
                    throw new NotSupportedException($"Tombstone type '{tombstoneType}' is not supported.");
            }

            return result;
        }

        internal List<PeriodicBackupInfo> GetPeriodicBackupsInformation()
        {
            return PeriodicBackups
                .Select(x => new PeriodicBackupInfo
                {
                    Database = _database.Name,
                    TaskId = x.Configuration.TaskId,
                    Name = x.Configuration.Name,
                    FullBackupFrequency = x.Configuration.FullBackupFrequency,
                    IncrementalBackupFrequency = x.Configuration.IncrementalBackupFrequency,
                    NextBackup = x.GetNextBackup(),
                    CreatedAt = x.GetCreatedAt(),
                    Disposed = x.Disposed
                })
                .ToList();
        }

        public Dictionary<TombstoneDeletionBlockageSource, HashSet<string>> GetDisabledSubscribersCollections(HashSet<string> tombstoneCollections)
        {
            var dict = new Dictionary<TombstoneDeletionBlockageSource, HashSet<string>>();

            foreach (var config in PeriodicBackups.Select(x => x.Configuration).Where(config => config.Disabled))
            {
                var source = new TombstoneDeletionBlockageSource(ITombstoneAware.TombstoneDeletionBlockerType.Backup, config.Name, config.TaskId);
                dict[source] = tombstoneCollections;
            }

            return dict;
        }

        public void HandleDatabaseValueChanged(string type, object changeState)
        {
            switch (type)
            {
                case nameof(UpdateResponsibleNodeForTasksCommand):
                    using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                    using (context.OpenReadTransaction())
                    using (var rawRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, _database.Name))
                    {
                        UpdateConfigurations(rawRecord.PeriodicBackups);
                    }
                    break;

                case nameof(DelayBackupCommand):
                    var state = (DelayBackupCommand.DelayBackupCommandState)changeState;
                    if (_periodicBackups.TryGetValue(state.TaskId, out var periodicBackup) == false)
                        throw new InvalidOperationException($"Backup task id: {state.TaskId} doesn't exist");

                    periodicBackup.BackupStatus ??= new PeriodicBackupStatus();
                    periodicBackup.BackupStatus.DelayUntil = state.DelayUntil;
                    periodicBackup.BackupStatus.OriginalBackupTime = state.OriginalBackupTime;
                    break;
            }
        }

        internal TestingStuff _forTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (_forTestingPurposes != null)
                return _forTestingPurposes;

            return _forTestingPurposes = new TestingStuff();
        }

        public sealed class TestingStuff
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
        }
    }
}
