using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Config.Settings;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;
using System.Collections.Concurrent;
using System.Linq;
using NCrontab.Advanced;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Sparrow.Collections;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class PeriodicBackupRunner : ITombstoneAware, IDisposable
    {
        private readonly Logger _logger;

        private readonly DocumentDatabase _database;
        private readonly ServerStore _serverStore;
        private readonly CancellationTokenSource _cancellationToken;
        private readonly PathSetting _tempBackupPath;

        private readonly ConcurrentDictionary<long, PeriodicBackup> _periodicBackups
            = new ConcurrentDictionary<long, PeriodicBackup>();

        private static readonly Dictionary<string, long> EmptyDictionary = new Dictionary<string, long>();

        private readonly ConcurrentSet<Task> _inactiveRunningPeriodicBackupsTasks = new ConcurrentSet<Task>();
        private bool _disposed;

        // interval can be 2^32-2 milliseconds at most
        // this is the maximum interval acceptable in .Net's threading timer
        public readonly TimeSpan MaxTimerTimeout = TimeSpan.FromMilliseconds(Math.Pow(2, 32) - 2);

        public ICollection<PeriodicBackup> PeriodicBackups => _periodicBackups.Values;

        public PeriodicBackupRunner(DocumentDatabase database, ServerStore serverStore)
        {
            _database = database;
            _serverStore = serverStore;
            _logger = LoggingSource.Instance.GetLogger<PeriodicBackupRunner>(_database.Name);
            _cancellationToken = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);

            _tempBackupPath = (_database.Configuration.Storage.TempPath ?? _database.Configuration.Core.DataDirectory).Combine("PeriodicBackupTemp");

            _database.TombstoneCleaner.Subscribe(this);
            IOExtensions.DeleteDirectory(_tempBackupPath.FullPath);
            Directory.CreateDirectory(_tempBackupPath.FullPath);
        }

        private Timer GetTimer(
            PeriodicBackupConfiguration configuration,
            PeriodicBackupStatus backupStatus)
        {
            var nextBackup = GetNextBackupDetails(configuration, backupStatus);
            if (nextBackup == null)
                return null;

            if (_logger.IsInfoEnabled)
                _logger.Info($"Next {(nextBackup.IsFull ? "full" : "incremental")} " +
                             $"backup is in {nextBackup.TimeSpan.TotalMinutes} minutes");

            var backupTaskDetails = new BackupTaskDetails
            {
                IsFullBackup = nextBackup.IsFull,
                TaskId = configuration.TaskId,
                NextBackup = nextBackup.TimeSpan
            };

            var isValidTimeSpanForTimer = IsValidTimeSpanForTimer(backupTaskDetails.NextBackup);
            var timer = isValidTimeSpanForTimer
                ? new Timer(TimerCallback, backupTaskDetails, backupTaskDetails.NextBackup, Timeout.InfiniteTimeSpan)
                : new Timer(LongPeriodTimerCallback, backupTaskDetails, MaxTimerTimeout, Timeout.InfiniteTimeSpan);

            return timer;
        }

        public NextBackup GetNextBackupDetails(
            DatabaseRecord databaseRecord,
            PeriodicBackupConfiguration configuration,
            PeriodicBackupStatus backupStatus)
        {
            var taskStatus = GetTaskStatus(databaseRecord, configuration, skipErrorLog: true);
            return taskStatus == TaskStatus.Disabled ? null : GetNextBackupDetails(configuration, backupStatus, skipErrorLog: true);
        }

        private NextBackup GetNextBackupDetails(
            PeriodicBackupConfiguration configuration,
            PeriodicBackupStatus backupStatus,
            bool skipErrorLog = false)
        {
            var now = SystemTime.UtcNow;
            var lastFullBackup = backupStatus.LastFullBackupInternal ?? now;
            var lastIncrementalBackup = backupStatus.LastIncrementalBackupInternal ?? backupStatus.LastFullBackupInternal ?? now;
            var nextFullBackup = GetNextBackupOccurrence(configuration.FullBackupFrequency,
                lastFullBackup, configuration, skipErrorLog: skipErrorLog);
            var nextIncrementalBackup = GetNextBackupOccurrence(configuration.IncrementalBackupFrequency,
                lastIncrementalBackup, configuration, skipErrorLog: skipErrorLog);

            if (nextFullBackup == null && nextIncrementalBackup == null)
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

                return null;
            }

            Debug.Assert(configuration.TaskId != 0);

            var isFullBackup = IsFullBackup(backupStatus, configuration, nextFullBackup, nextIncrementalBackup);
            var nextBackupDateTime = GetNextBackupDateTime(nextFullBackup, nextIncrementalBackup);
            var nowLocalTime = now.ToLocalTime();
            var nextBackupTimeSpan = (nextBackupDateTime - nowLocalTime).Ticks <= 0 ? TimeSpan.Zero : nextBackupDateTime - nowLocalTime;

            return new NextBackup
            {
                TimeSpan = nextBackupTimeSpan,
                DateTime = DateTime.UtcNow.Add(nextBackupTimeSpan),
                IsFull = isFullBackup
            };
        }

        private bool IsFullBackup(PeriodicBackupStatus backupStatus,
            PeriodicBackupConfiguration configuration,
            DateTime? nextFullBackup, DateTime? nextIncrementalBackup)
        {
            if (backupStatus.LastFullBackup == null ||
                backupStatus.NodeTag != _serverStore.NodeTag ||
                backupStatus.BackupType != configuration.BackupType ||
                backupStatus.LastEtag == null)
            {
                // Reasons to start a new full backup:
                // 1. there is no previous full backup, we are going to create one now
                // 2. the node which is responsible for the backup was replaced
                // 3. the backup type changed (e.g. from backup to snapshot)
                // 4. last etag wasn't updated

                return true;
            }

            // 1. there is a full backup setup but the next incremental backup wasn't setup
            // 2. there is a full backup setup and the next full backup is before the incremental one
            return nextFullBackup != null &&
                   (nextIncrementalBackup == null || nextFullBackup <= nextIncrementalBackup);
        }

        private static bool IsFullBackupOrSnapshot(string filePath)
        {
            var extension = Path.GetExtension(filePath);
            return Constants.Documents.PeriodicBackup.FullBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                   Constants.Documents.PeriodicBackup.SnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);
        }

        private static DateTime GetNextBackupDateTime(DateTime? nextFullBackup, DateTime? nextIncrementalBackup)
        {
            Debug.Assert(nextFullBackup != null || nextIncrementalBackup != null);

            if (nextFullBackup == null)
                return nextIncrementalBackup.Value;

            if (nextIncrementalBackup == null)
                return nextFullBackup.Value;

            var nextBackup =
                nextFullBackup <= nextIncrementalBackup ? nextFullBackup.Value : nextIncrementalBackup.Value;

            return nextBackup;
        }

        private DateTime? GetNextBackupOccurrence(string backupFrequency,
            DateTime lastBackupUtc, PeriodicBackupConfiguration configuration, bool skipErrorLog)
        {
            if (string.IsNullOrWhiteSpace(backupFrequency))
                return null;

            try
            {
                var backupParser = CrontabSchedule.Parse(backupFrequency);
                return backupParser.GetNextOccurrence(lastBackupUtc.ToLocalTime());
            }
            catch (Exception e)
            {
                if (skipErrorLog == false)
                {
                    var message = "Couldn't parse periodic backup " +
                                  $"frequency {backupFrequency}, task id: {configuration.TaskId}";
                    if (string.IsNullOrWhiteSpace(configuration.Name) == false)
                        message += $", backup name: {configuration.Name}";

                    message += $", error: {e.Message}";

                    if (_logger.IsInfoEnabled)
                        _logger.Info(message);

                    _database.NotificationCenter.Add(AlertRaised.Create(
                        _database.Name,
                        "Backup frequency parsing error",
                        message,
                        AlertType.PeriodicBackup,
                        NotificationSeverity.Error,
                        details: new ExceptionDetails(e)));
                }

                return null;
            }
        }

        private class BackupTaskDetails
        {
            public long TaskId { get; set; }

            public bool IsFullBackup { get; set; }

            public TimeSpan NextBackup { get; set; }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsValidTimeSpanForTimer(TimeSpan nextBackupTimeSpan)
        {
            return nextBackupTimeSpan < MaxTimerTimeout;
        }

        private void TimerCallback(object backupTaskDetails)
        {
            try
            {
                if (_cancellationToken.IsCancellationRequested)
                    return;

                var backupDetails = (BackupTaskDetails)backupTaskDetails;

                if (ShouldRunBackupAfterTimerCallback(backupDetails, out PeriodicBackup periodicBackup) == false)
                    return;

                CreateBackupTask(periodicBackup, backupDetails.IsFullBackup);
            }
            catch (Exception e)
            {
                _logger.Operations("Error during timer callback", e);
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

            var databaseRecord = GetDatabaseRecord();
            var backupStatus = GetBackupStatus(taskId);
            return _database.WhoseTaskIsIt(databaseRecord.Topology, periodicBackup.Configuration, backupStatus, useLastResponsibleNodeIfNoAvailableNodes: true);
        }

        public long StartBackupTask(long taskId, bool isFullBackup)
        {
            if (_periodicBackups.TryGetValue(taskId, out var periodicBackup) == false)
            {
                throw new InvalidOperationException($"Backup task id: {taskId} doesn't exist");
            }

            return CreateBackupTask(periodicBackup, isFullBackup);
        }

        private long CreateBackupTask(PeriodicBackup periodicBackup, bool isFullBackup)
        {
            if (periodicBackup.UpdateBackupTaskSemaphore.Wait(0) == false)
                return periodicBackup.RunningBackupTaskId ?? -1;

            try
            {
                if (periodicBackup.RunningTask != null)
                    return periodicBackup.RunningBackupTaskId ?? -1;

                var backupStatus = periodicBackup.BackupStatus = GetBackupStatus(periodicBackup.Configuration.TaskId, periodicBackup.BackupStatus);
                var backupToLocalFolder = PeriodicBackupConfiguration.CanBackupUsing(periodicBackup.Configuration.LocalSettings);

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

                var operationId = _database.Operations.GetNextOperationId();
                var backupTypeText = GetBackupTypeText(isFullBackup, periodicBackup.Configuration.BackupType);

                periodicBackup.StartTime = SystemTime.UtcNow;
                var backupTask = new BackupTask(
                    _serverStore,
                    _database,
                    periodicBackup,
                    isFullBackup,
                    backupToLocalFolder,
                    operationId,
                    _tempBackupPath,
                    _logger,
                    _cancellationToken.Token);

                periodicBackup.RunningBackupTaskId = operationId;
                periodicBackup.CancelToken = backupTask.TaskCancelToken;
                var backupTaskName = $"{backupTypeText} backup task: '{periodicBackup.Configuration.Name}'";

                var task = _database.Operations.AddOperation(
                    null,
                    backupTaskName,
                    Operations.Operations.OperationType.DatabaseBackup,
                    taskFactory: onProgress => Task.Run(async () =>
                    {
                        try
                        {
                            return await backupTask.RunPeriodicBackup(onProgress);
                        }
                        finally
                        {
                            periodicBackup.RunningTask = null;
                            periodicBackup.RunningBackupTaskId = null;
                            periodicBackup.CancelToken = null;
                            periodicBackup.RunningBackupStatus = null;

                            if (periodicBackup.HasScheduledBackup() &&
                                _cancellationToken.IsCancellationRequested == false)
                            {
                                var newBackupTimer = GetTimer(periodicBackup.Configuration, periodicBackup.BackupStatus);
                                periodicBackup.UpdateTimer(newBackupTimer, discardIfDisabled: true);
                            }
                        }
                    }, backupTask.TaskCancelToken.Token),
                    id: operationId,
                    token: backupTask.TaskCancelToken);

                periodicBackup.RunningTask = task;
                task.ContinueWith(_ => backupTask.TaskCancelToken.Dispose());

                return operationId;
            }
            catch (Exception e)
            {
                var message = $"Failed to start the backup task: '{periodicBackup.Configuration.Name}'";
                if (_logger.IsOperationsEnabled)
                    _logger.Operations(message, e);

                _database.NotificationCenter.Add(AlertRaised.Create(
                    _database.Name,
                    $"Periodic Backup task: '{periodicBackup.Configuration.Name}'",
                    message,
                    AlertType.PeriodicBackup,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e)));

                throw;
            }
            finally
            {
                periodicBackup.UpdateBackupTaskSemaphore.Release();
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

        private DatabaseRecord GetDatabaseRecord()
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return _serverStore.Cluster.ReadDatabase(context, _database.Name);
            }
        }

        private void LongPeriodTimerCallback(object backupTaskDetails)
        {
            try
            {
                if (_cancellationToken.IsCancellationRequested)
                    return;

                var backupDetails = (BackupTaskDetails)backupTaskDetails;

                if (ShouldRunBackupAfterTimerCallback(backupDetails, out PeriodicBackup periodicBackup) == false)
                    return;

                var remainingInterval = backupDetails.NextBackup - MaxTimerTimeout;
                if (remainingInterval.TotalMilliseconds <= 0)
                {
                    CreateBackupTask(periodicBackup, backupDetails.IsFullBackup);
                    return;
                }

                periodicBackup.UpdateTimer(GetTimer(periodicBackup.Configuration, periodicBackup.BackupStatus));
            }
            catch (Exception e)
            {
                _logger.Operations("Error during long timer callback", e);
            }
        }

        private bool ShouldRunBackupAfterTimerCallback(BackupTaskDetails backupInfo, out PeriodicBackup periodicBackup)
        {
            if (_periodicBackups.TryGetValue(backupInfo.TaskId, out periodicBackup) == false)
            {
                // periodic backup doesn't exist anymore
                return false;
            }

            var databaseRecord = GetDatabaseRecord();
            if (databaseRecord == null)
                return false;

            var taskStatus = GetTaskStatus(databaseRecord, periodicBackup.Configuration);
            return taskStatus == TaskStatus.ActiveByCurrentNode;
        }

        public PeriodicBackupStatus GetBackupStatus(long taskId)
        {
            PeriodicBackupStatus inMemoryBackupStatus = null;
            if (_periodicBackups.TryGetValue(taskId, out PeriodicBackup periodicBackup))
                inMemoryBackupStatus = periodicBackup.BackupStatus;

            return GetBackupStatus(taskId, inMemoryBackupStatus);
        }

        private PeriodicBackupStatus GetBackupStatus(long taskId, PeriodicBackupStatus inMemoryBackupStatus)
        {
            var backupStatus = GetBackupStatusFromCluster(taskId);
            if (backupStatus == null)
            {
                backupStatus = inMemoryBackupStatus ?? new PeriodicBackupStatus
                {
                    TaskId = taskId
                };
            }
            else if (inMemoryBackupStatus?.Version > backupStatus.Version &&
                     inMemoryBackupStatus?.NodeTag == backupStatus.NodeTag)
            {
                // the in memory backup status is more updated
                // and is of the same node (current one)
                backupStatus = inMemoryBackupStatus;
            }

            return backupStatus;
        }

        private PeriodicBackupStatus GetBackupStatusFromCluster(long taskId)
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var statusBlittable = _serverStore.Cluster.Read(context, PeriodicBackupStatus.GenerateItemName(_database.Name, taskId));

                if (statusBlittable == null)
                    return null;

                var periodicBackupStatusJson = JsonDeserializationClient.PeriodicBackupStatus(statusBlittable);
                return periodicBackupStatusJson;
            }
        }

        public void UpdateConfigurations(DatabaseRecord databaseRecord)
        {
            if (_disposed)
                return;

            if (databaseRecord.PeriodicBackups == null)
            {
                foreach (var periodicBackup in _periodicBackups)
                {
                    periodicBackup.Value.DisableFutureBackups();

                    TryAddInactiveRunningPeriodicBackups(periodicBackup.Value.RunningTask);
                }
                return;
            }

            var allBackupTaskIds = new List<long>();
            foreach (var periodicBackupConfiguration in databaseRecord.PeriodicBackups)
            {
                var newBackupTaskId = periodicBackupConfiguration.TaskId;
                allBackupTaskIds.Add(newBackupTaskId);

                var taskState = GetTaskStatus(databaseRecord, periodicBackupConfiguration);

                UpdatePeriodicBackup(newBackupTaskId, periodicBackupConfiguration, taskState);
            }

            RemoveInactiveCompletedTasks();

            var deletedBackupTaskIds = _periodicBackups.Keys.Except(allBackupTaskIds).ToList();
            foreach (var deletedBackupId in deletedBackupTaskIds)
            {
                if (_periodicBackups.TryRemove(deletedBackupId, out var deletedBackup) == false)
                    continue;

                // stopping any future backups
                // currently running backups will continue to run
                deletedBackup.DisableFutureBackups();
                TryAddInactiveRunningPeriodicBackups(deletedBackup.RunningTask);
            }
        }

        public void RemoveInactiveCompletedTasks()
        {
            if (_inactiveRunningPeriodicBackupsTasks.Count == 0)
                return;

            var tasksToRemove = new List<Task>();
            foreach (var inactiveTask in _inactiveRunningPeriodicBackupsTasks)
            {
                if (inactiveTask.IsCompleted == false)
                    continue;

                tasksToRemove.Add(inactiveTask);
            }

            foreach (var taskToRemove in tasksToRemove)
            {
                _inactiveRunningPeriodicBackupsTasks.TryRemove(taskToRemove);
            }
        }

        private void UpdatePeriodicBackup(long taskId,
            PeriodicBackupConfiguration newConfiguration,
            TaskStatus taskState)
        {
            Debug.Assert(taskId == newConfiguration.TaskId);

            var backupStatus = GetBackupStatus(taskId, inMemoryBackupStatus: null);
            if (_periodicBackups.TryGetValue(taskId, out var existingBackupState) == false)
            {
                var newPeriodicBackup = new PeriodicBackup
                {
                    Configuration = newConfiguration
                };

                _periodicBackups.TryAdd(taskId, newPeriodicBackup);

                if (taskState == TaskStatus.ActiveByCurrentNode)
                    newPeriodicBackup.UpdateTimer(GetTimer(newConfiguration, backupStatus));

                return;
            }

            var previousConfiguration = existingBackupState.Configuration;
            existingBackupState.Configuration = newConfiguration;

            if (taskState != TaskStatus.ActiveByCurrentNode)
            {
                // this node isn't responsible for the backup task
                existingBackupState.DisableFutureBackups();
                return;
            }

            if (existingBackupState.RunningTask != null)
            {
                // a backup is already running 
                // the next one will be re-scheduled by the backup task
                return;
            }

            if (previousConfiguration.HasBackupFrequencyChanged(newConfiguration) == false &&
                existingBackupState.HasScheduledBackup())
            {
                // backup frequency hasn't changed
                // and we have a scheduled backup
                return;
            }

            existingBackupState.UpdateTimer(GetTimer(newConfiguration, backupStatus));
        }

        private enum TaskStatus
        {
            Disabled,
            ActiveByCurrentNode,
            ActiveByOtherNode
        }

        private TaskStatus GetTaskStatus(
            DatabaseRecord databaseRecord,
            PeriodicBackupConfiguration configuration,
            bool skipErrorLog = false)
        {
            if (configuration.Disabled)
                return TaskStatus.Disabled;

            if (configuration.HasBackup() == false)
            {
                if (skipErrorLog == false)
                {
                    var message = $"All backup destinations are disabled for backup task id: {configuration.TaskId}";
                    _database.NotificationCenter.Add(AlertRaised.Create(
                        _database.Name,
                        "Periodic Backup",
                        message,
                        AlertType.PeriodicBackup,
                        NotificationSeverity.Info));
                }

                return TaskStatus.Disabled;
            }

            var backupStatus = GetBackupStatus(configuration.TaskId);
            var whoseTaskIsIt = _database.WhoseTaskIsIt(databaseRecord.Topology, configuration, backupStatus, useLastResponsibleNodeIfNoAvailableNodes: true);
            if (whoseTaskIsIt == null)
                return TaskStatus.Disabled;

            if (whoseTaskIsIt == _serverStore.NodeTag)
                return TaskStatus.ActiveByCurrentNode;

            if (_logger.IsInfoEnabled)
                _logger.Info($"Backup job is skipped at {SystemTime.UtcNow}, because it is managed " +
                             $"by '{whoseTaskIsIt}' node and not the current node ({_serverStore.NodeTag})");

            return TaskStatus.ActiveByOtherNode;
        }

        private void TryAddInactiveRunningPeriodicBackups(Task runningTask)
        {
            if (runningTask == null ||
                runningTask.IsCompleted)
                return;

            _inactiveRunningPeriodicBackupsTasks.Add(runningTask);
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
                        periodicBackup.Value.DisableFutureBackups();

                        var task = periodicBackup.Value.RunningTask;
                        WaitForTaskCompletion(task);
                    }

                    foreach (var task in _inactiveRunningPeriodicBackupsTasks)
                    {
                        WaitForTaskCompletion(task);
                    }
                }

                if (_tempBackupPath != null)
                    IOExtensions.DeleteDirectory(_tempBackupPath.FullPath);
            }
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
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error when disposing periodic backup runner task", e);
            }
        }

        public bool HasPeriodicBackups()
        {
            RemoveInactiveCompletedTasks();
            return _periodicBackups.Count > 0 || _inactiveRunningPeriodicBackupsTasks.Count > 0;
        }

        public bool HasRunningBackups()
        {
            foreach (var periodicBackup in _periodicBackups)
            {
                if (periodicBackup.Value.RunningTask != null &&
                    periodicBackup.Value.RunningTask.IsCompleted == false)
                    return true;
            }

            RemoveInactiveCompletedTasks();

            return _inactiveRunningPeriodicBackupsTasks.Count > 0;
        }

        public BackupInfo GetBackupInfo()
        {
            if (_periodicBackups.Count == 0)
                return null;

            var allBackupTicks = new List<long>();
            var allNextBackupTimeSpanSeconds = new List<double>();
            foreach (var periodicBackup in _periodicBackups)
            {
                var configuration = periodicBackup.Value.Configuration;
                var backupStatus = GetBackupStatus(configuration.TaskId, periodicBackup.Value.BackupStatus);
                if (backupStatus == null)
                    continue;

                if (backupStatus.LastFullBackup != null)
                    allBackupTicks.Add(backupStatus.LastFullBackup.Value.Ticks);

                if (backupStatus.LastIncrementalBackup != null)
                    allBackupTicks.Add(backupStatus.LastIncrementalBackup.Value.Ticks);

                var nextBackup = GetNextBackupDetails(configuration, backupStatus, skipErrorLog: true);
                if (nextBackup != null)
                {
                    allNextBackupTimeSpanSeconds.Add(nextBackup.TimeSpan.TotalSeconds);
                }
            }

            return new BackupInfo
            {
                LastBackup = allBackupTicks.Count == 0 ? (DateTime?)null : new DateTime(allBackupTicks.Max()),
                IntervalUntilNextBackupInSec = allNextBackupTimeSpanSeconds.Count == 0 ? 0 : allNextBackupTimeSpanSeconds.Min()
            };
        }

        public RunningBackup OnGoingBackup(long taskId)
        {
            if (_periodicBackups.TryGetValue(taskId, out var periodicBackup) == false)
                return null;

            if (periodicBackup.RunningTask == null)
                return null;

            return new RunningBackup
            {
                StartTime = periodicBackup.StartTime,
                IsFull = periodicBackup.RunningBackupStatus?.IsFull ?? false,
                RunningBackupTaskId = periodicBackup.RunningBackupTaskId
            };
        }

        public Dictionary<string, long> GetLastProcessedTombstonesPerCollection()
        {
            if (_periodicBackups.Count == 0)
                return EmptyDictionary;

            var processedTombstonesPerCollection = new Dictionary<string, long>();

            var minLastEtag = long.MaxValue;
            foreach (var periodicBackup in _periodicBackups.Values)
            {
                if (periodicBackup.BackupStatus?.LastEtag != null &&
                    minLastEtag > periodicBackup.BackupStatus?.LastEtag)
                {
                    minLastEtag = periodicBackup.BackupStatus.LastEtag.Value;
                }
            }

            if (minLastEtag == long.MaxValue)
                minLastEtag = 0;

            processedTombstonesPerCollection[Constants.Documents.Collections.AllDocumentsCollection] = minLastEtag;

            return processedTombstonesPerCollection;
        }
    }
}
