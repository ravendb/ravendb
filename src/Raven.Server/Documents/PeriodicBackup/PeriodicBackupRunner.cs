using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NCrontab.Advanced;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config.Settings;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Utils;
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
            _tempBackupPath = (_database.Configuration.Storage.TempPath ?? _database.Configuration.Core.DataDirectory).Combine("PeriodicBackupTemp");

            // we pass wakeup-1 to ensure the backup will run right after DB woke up on wakeup time, and not on the next occurrence.
            // relevant only if it's the first backup after waking up
            _databaseWakeUpTimeUtc = wakeup?.AddMinutes(-1);

            _database.TombstoneCleaner.Subscribe(this);
            IOExtensions.DeleteDirectory(_tempBackupPath.FullPath);
            Directory.CreateDirectory(_tempBackupPath.FullPath);
        }

        public NextBackup GetNextBackupDetails(
            DatabaseRecord databaseRecord,
            PeriodicBackupConfiguration configuration,
            PeriodicBackupStatus backupStatus,
            string responsibleNodeTag)
        {
            var taskStatus = GetTaskStatus(databaseRecord.Topology, configuration, disableLog: true);
            return taskStatus == TaskStatus.Disabled ? null : GetNextBackupDetails(configuration, backupStatus, responsibleNodeTag, skipErrorLog: true);
        }

        private DateTime? GetNextWakeupTimeLocal(string databaseName, long lastEtag, PeriodicBackupConfiguration configuration, TransactionOperationContext context)
        {
            // we will always wake up the database for a full backup.
            // but for incremental we will wake the database only if there were changes made.

            if (configuration.Disabled || configuration.IncrementalBackupFrequency == null && configuration.FullBackupFrequency == null || configuration.HasBackup() == false)
                return null;

            var backupStatus = GetBackupStatusFromCluster(_serverStore, context, databaseName, configuration.TaskId);
            if (backupStatus == null)
            {
                // we want to wait for the backup occurrence
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Backup Task '{configuration.TaskId}' of database '{databaseName}' is never backed up yet.");

                return DateTime.UtcNow;
            }

            var topology = _serverStore.LoadDatabaseTopology(_database.Name);
            var responsibleNodeTag = _database.WhoseTaskIsIt(topology, configuration, backupStatus, keepTaskOnOriginalMemberNode: true);
            if (responsibleNodeTag == null)
            {
                // cluster is down
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Could not find the responsible node for backup task '{configuration.TaskId}' of database '{databaseName}'.");

                return DateTime.UtcNow;
            }

            if (responsibleNodeTag != _serverStore.NodeTag)
            {
                // not responsive for this backup task
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Current server '{_serverStore.NodeTag}' is not responsible node for backup task '{configuration.TaskId}' of database '{databaseName}'. Backup Task responsible node is '{responsibleNodeTag}'.");

                return null;
            }

            var nextBackup = GetNextBackupDetails(configuration, backupStatus, _serverStore.NodeTag);
            if (nextBackup == null)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Backup Task '{configuration.TaskId}' of database '{databaseName}' doesn't have next backup. Should not happen and likely a bug.");

                return null;
            }

            var nowUtc = SystemTime.UtcNow;
            if (nextBackup.DateTime < nowUtc)
            {
                // this backup is delayed
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Backup Task '{configuration.TaskId}' of database '{databaseName}' is delayed.");
                return DateTime.UtcNow;
            }

            if (backupStatus.LastEtag != lastEtag)
            {
                // we have changes since last backup
                var type = nextBackup.IsFull ? "full" : "incremental";
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Backup Task '{configuration.TaskId}' of database '{databaseName}' have changes since last backup. Wakeup timer will be set to the next {type} backup at '{nextBackup.DateTime}'.");
                return nextBackup.DateTime;
            }

            if (nextBackup.IsFull)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Backup Task '{configuration.TaskId}' of database '{databaseName}' doesn't have changes since last backup. Wakeup timer will be set to the next full backup at '{nextBackup.DateTime}'.");
                return nextBackup.DateTime;
            }

            // we don't have changes since the last backup and the next backup is incremental
            var lastFullBackup = backupStatus.LastFullBackupInternal ?? nowUtc;
            var nextFullBackup = GetNextBackupOccurrenceLocal(configuration.FullBackupFrequency, lastFullBackup, configuration, skipErrorLog: false);
            if (nextFullBackup < nowUtc)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Backup Task '{configuration.TaskId}' of database '{databaseName}' doesn't have changes since last backup but has delayed backup.");
                // this backup is delayed
                return DateTime.UtcNow;
            }

            if (_logger.IsOperationsEnabled)
                _logger.Operations($"Backup Task '{configuration.TaskId}' of database '{databaseName}' doesn't have changes since last backup. Wakeup timer set to next full backup at {nextFullBackup}, and will skip the incremental backups.");

            return nextFullBackup;
        }

        private NextBackup GetNextBackupDetails(
            PeriodicBackupConfiguration configuration,
            PeriodicBackupStatus backupStatus,
            string responsibleNodeTag,
            bool skipErrorLog = false)
        {
            var nowUtc = SystemTime.UtcNow;
            var lastFullBackupUtc = backupStatus.LastFullBackupInternal ?? _databaseWakeUpTimeUtc ?? nowUtc;
            var lastIncrementalBackupUtc = backupStatus.LastIncrementalBackupInternal ?? backupStatus.LastFullBackupInternal ?? _databaseWakeUpTimeUtc ?? nowUtc;
            var nextFullBackup = GetNextBackupOccurrenceLocal(configuration.FullBackupFrequency,
                lastFullBackupUtc, configuration, skipErrorLog: skipErrorLog);
            var nextIncrementalBackup = GetNextBackupOccurrenceLocal(configuration.IncrementalBackupFrequency,
                lastIncrementalBackupUtc, configuration, skipErrorLog: skipErrorLog);

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

            var isFullBackup = IsFullBackup(backupStatus, configuration, nextFullBackup, nextIncrementalBackup, responsibleNodeTag);
            var nextBackupTimeLocal = GetNextBackupDateTime(nextFullBackup, nextIncrementalBackup);
            var nowLocalTime = SystemTime.UtcNow.ToLocalTime();
            var timeSpan = nextBackupTimeLocal - nowLocalTime;

            TimeSpan nextBackupTimeSpan;
            if (timeSpan.Ticks <= 0)
            {
                // overdue backup of current node or first backup
                if (backupStatus.NodeTag == _serverStore.NodeTag || backupStatus.NodeTag == null)
                {
                    // the backup will run now
                    nextBackupTimeSpan = TimeSpan.Zero;
                    nextBackupTimeLocal = nowLocalTime;
                }
                else
                {
                    // overdue backup from other node
                    nextBackupTimeSpan = TimeSpan.FromMinutes(1);
                    nextBackupTimeLocal = nowLocalTime + nextBackupTimeSpan;
                }
            }
            else
            {
                nextBackupTimeSpan = timeSpan;
            }

            return new NextBackup
            {
                TimeSpan = nextBackupTimeSpan,
                DateTime = nextBackupTimeLocal.ToUniversalTime(),
                IsFull = isFullBackup,
                TaskId = configuration.TaskId
            };
        }

        private bool IsFullBackup(PeriodicBackupStatus backupStatus,
            PeriodicBackupConfiguration configuration,
            DateTime? nextFullBackup, DateTime? nextIncrementalBackup, string responsibleNodeTag)
        {
            if (backupStatus.LastFullBackup == null ||
                backupStatus.NodeTag != responsibleNodeTag ||
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
                   Constants.Documents.PeriodicBackup.SnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                   Constants.Documents.PeriodicBackup.EncryptedFullBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                   Constants.Documents.PeriodicBackup.EncryptedSnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);
        }

        private static DateTime GetNextBackupDateTime(DateTime? nextFullBackup, DateTime? nextIncrementalBackup)
        {
            Debug.Assert(nextFullBackup != null || nextIncrementalBackup != null);

            if (nextFullBackup == null)
                return nextIncrementalBackup.Value;

            if (nextIncrementalBackup == null)
                return nextFullBackup.Value;

            var nextBackup = nextFullBackup <= nextIncrementalBackup ? nextFullBackup.Value : nextIncrementalBackup.Value;
            return nextBackup;
        }

        private DateTime? GetNextBackupOccurrenceLocal(string backupFrequency,
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

                    if (_logger.IsOperationsEnabled)
                        _logger.Operations(message);

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

            var topology = _serverStore.LoadDatabaseTopology(_database.Name);
            var backupStatus = GetBackupStatus(taskId);
            return _database.WhoseTaskIsIt(topology, periodicBackup.Configuration, backupStatus, keepTaskOnOriginalMemberNode: true);
        }

        public long StartBackupTask(long taskId, bool isFullBackup)
        {
            if (_periodicBackups.TryGetValue(taskId, out var periodicBackup) == false)
            {
                throw new InvalidOperationException($"Backup task id: {taskId} doesn't exist");
            }

            return CreateBackupTask(periodicBackup, isFullBackup, SystemTime.UtcNow);
        }

        public DateTime? GetWakeDatabaseTimeUtc(string databaseName)
        {
            if (_periodicBackups.Count == 0)
                return null;

            long lastEtag;

            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                lastEtag = DocumentsStorage.ReadLastEtag(tx.InnerTransaction);
            }

            DateTime? wakeupDatabase = null;
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var backup in _periodicBackups)
                {
                    var nextBackup = GetNextWakeupTimeLocal(databaseName, lastEtag, backup.Value.Configuration, context);
                    if (nextBackup == null)
                        continue;

                    if (wakeupDatabase == null)
                    {
                        // first time
                        wakeupDatabase = nextBackup;
                    }
                    else if (nextBackup < wakeupDatabase)
                    {
                        // next backup is earlier than the current one
                        wakeupDatabase = nextBackup.Value;
                    }
                }
            }

            return wakeupDatabase?.ToUniversalTime();
        }

        private long CreateBackupTask(PeriodicBackup periodicBackup, bool isFullBackup, DateTime startTimeInUtc)
        {
            using (periodicBackup.UpdateBackupTask())
            {
                if (periodicBackup.Disposed)
                    throw new InvalidOperationException("Backup task was already disposed");

                var runningTask = periodicBackup.RunningTask;
                if (runningTask != null)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Could not start backup task '{periodicBackup.Configuration.TaskId}' because there is already a running backup '{runningTask.Id}'");

                    return runningTask.Id;
                }

                CheckServerHealthBeforeBackup(_serverStore, periodicBackup.Configuration.Name);
                _serverStore.ConcurrentBackupsCounter.StartBackup(periodicBackup.Configuration.Name, _logger);

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

                    var operationId = _database.Operations.GetNextOperationId();
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
                        OperationId = operationId,
                        Name = periodicBackup.Configuration.Name
                    };

                    var backupTask = new BackupTask(_database, backupParameters, periodicBackup.Configuration, _logger, _forTestingPurposes);
                    periodicBackup.CancelToken = backupTask.TaskCancelToken;

                    periodicBackup.RunningTask = new PeriodicBackup.RunningBackupTask
                    {
                        Id = operationId,
                        Task = tcs.Task
                    };

                    var task = _database.Operations.AddOperation(
                        null,
                        $"{backupTypeText} backup task: '{periodicBackup.Configuration.Name}'. Database: '{_database.Name}'",
                        Operations.Operations.OperationType.DatabaseBackup,
                        taskFactory: onProgress => StartBackupThread(periodicBackup, backupTask, tcs, onProgress),
                        id: operationId,
                        token: backupTask.TaskCancelToken);

                    task.ContinueWith(_ => backupTask.TaskCancelToken.Dispose());

                    return operationId;
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

                    BackupTask.SaveBackupStatus(periodicBackup.BackupStatus, _database, _logger);

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

        internal static void CheckServerHealthBeforeBackup(ServerStore serverStore, string name)
        {
            if (serverStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised())
            {
                throw new BackupDelayException($"Failed to start Backup Task: '{name}'. The task cannot run because the CPU credits allocated to this machine are nearing exhaustion.")
                {
                    DelayPeriod = serverStore.Configuration.Server.CpuCreditsExhaustionBackupDelay.AsTimeSpan
                };
            }

            if (LowMemoryNotification.Instance.LowMemoryState)
            {
                throw new BackupDelayException($"Failed to start Backup Task: '{name}'. The task cannot run because the server is in low memory state.")
                {
                    DelayPeriod = serverStore.Configuration.Backup.LowMemoryBackupDelay.AsTimeSpan
                };
            }

            if (LowMemoryNotification.Instance.DirtyMemoryState.IsHighDirty)
            {
                throw new BackupDelayException($"Failed to start Backup Task: '{name}'. The task cannot run because the server is in high dirty memory state.")
                {
                    DelayPeriod = serverStore.Configuration.Backup.LowMemoryBackupDelay.AsTimeSpan
                };
            }
        }

        private Task<IOperationResult> StartBackupThread(PeriodicBackup periodicBackup, BackupTask backupTask, TaskCompletionSource<IOperationResult> tcs, Action<IOperationProgress> onProgress)
        {
            PoolOfThreads.GlobalRavenThreadPool.LongRunning(x => RunBackupThread(periodicBackup, backupTask, tcs, onProgress), null, $"Backup task {periodicBackup.Configuration.Name} for database '{_database.Name}'");
            return tcs.Task;
        }

        private void RunBackupThread(PeriodicBackup periodicBackup, BackupTask backupTask, TaskCompletionSource<IOperationResult> tcs, Action<IOperationProgress> onProgress)
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
                Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;
                NativeMemory.EnsureRegistered();

                using (_database.PreventFromUnloading())
                {
                    backupResult = (BackupResult)backupTask.RunPeriodicBackup(onProgress, ref runningBackupStatus);
                    tcs.SetResult(backupResult);
                }
            }
            catch (OperationCanceledException oce)
            {
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
                _serverStore.ConcurrentBackupsCounter.FinishBackup(periodicBackup.Configuration.Name, periodicBackup.RunningBackupStatus, elapsed, _logger);

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

            DatabaseTopology topology;
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

                topology = rawRecord.Topology;
            }

            var taskStatus = GetTaskStatus(topology, periodicBackup.Configuration);
            if (_forTestingPurposes != null)
            {
                if (_forTestingPurposes.SimulateClusterDownStatus)
                {
                    taskStatus = TaskStatus.ClusterDown;
                    _forTestingPurposes.ClusterDownStatusSimulated = true;
                }
                else if (_forTestingPurposes.SimulateActiveByOtherNodeStatus)
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
                case TaskStatus.ClusterDown:
                    msg = $"Backup {backupInfo.TaskId}, current status is {taskStatus}, the backup will be rescheduled on current node.";
                    var status = GetBackupStatus(backupInfo.TaskId, periodicBackup.BackupStatus);
                    periodicBackup.UpdateTimer(GetNextBackupDetails(periodicBackup.Configuration, status, _serverStore.NodeTag), lockTaken: false);
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

            return GetBackupStatus(taskId, inMemoryBackupStatus);
        }

        private PeriodicBackupStatus GetBackupStatus(long taskId, PeriodicBackupStatus inMemoryBackupStatus)
        {
            var backupStatus = GetBackupStatusFromCluster(_serverStore, _database.Name, taskId);
            return ComparePeriodicBackupStatus(taskId, backupStatus, inMemoryBackupStatus);
        }

        private static PeriodicBackupStatus ComparePeriodicBackupStatus(long taskId, PeriodicBackupStatus backupStatus, PeriodicBackupStatus inMemoryBackupStatus)
        {
            if (backupStatus == null)
            {
                backupStatus = inMemoryBackupStatus ?? new PeriodicBackupStatus {TaskId = taskId};
            }
            else if (inMemoryBackupStatus?.Version > backupStatus.Version && inMemoryBackupStatus.NodeTag == backupStatus.NodeTag)
            {
                // the in memory backup status is more updated
                // and is of the same node (current one)
                backupStatus = inMemoryBackupStatus;
            }

            return backupStatus;
        }

        private static PeriodicBackupStatus GetBackupStatusFromCluster(ServerStore serverStore, string databaseName, long taskId)
        {
            using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return GetBackupStatusFromCluster(serverStore, context, databaseName, taskId);
            }
        }

        internal static PeriodicBackupStatus GetBackupStatusFromCluster(ServerStore serverStore, TransactionOperationContext context, string databaseName, long taskId)
        {
            var statusBlittable = serverStore.Cluster.Read(context, PeriodicBackupStatus.GenerateItemName(databaseName, taskId));

            if (statusBlittable == null)
                return null;

            var periodicBackupStatusJson = JsonDeserializationClient.PeriodicBackupStatus(statusBlittable);
            return periodicBackupStatusJson;
        }

        private long GetMinLastEtag()
        {
            var min = long.MaxValue;

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
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
                    var status = GetBackupStatusFromCluster(_serverStore, context, _database.Name, taskId);
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

        public void UpdateConfigurations(DatabaseRecord databaseRecord)
        {
            if (_disposed)
                return;

            if (databaseRecord.PeriodicBackups == null)
            {
                foreach (var periodicBackup in _periodicBackups)
                {
                    periodicBackup.Value.Dispose();
                }
                _periodicBackups.Clear();
                return;
            }

            var allBackupTaskIds = new List<long>();
            foreach (var periodicBackupConfiguration in databaseRecord.PeriodicBackups)
            {
                var newBackupTaskId = periodicBackupConfiguration.TaskId;
                allBackupTaskIds.Add(newBackupTaskId);

                var taskState = GetTaskStatus(databaseRecord.Topology, periodicBackupConfiguration);

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
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"New backup task '{taskId}' state is '{taskState}', will arrange a new backup timer.");

                    var backupStatus = GetBackupStatus(taskId, inMemoryBackupStatus: null);
                    periodicBackup.UpdateTimer(GetNextBackupDetails(newConfiguration, backupStatus, _serverStore.NodeTag), lockTaken: false);
                }

                return;
            }

            var previousConfiguration = existingBackupState.Configuration;
            existingBackupState.Configuration = newConfiguration;

            switch (taskState)
            {
                case TaskStatus.Disabled:
                case TaskStatus.ActiveByOtherNode:
                    // the task is disabled or this node isn't responsible for the backup task
                    existingBackupState.DisableFutureBackups();

                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Backup task '{taskId}' state is '{taskState}', will cancel the timer for it.");

                    return;
                case TaskStatus.ClusterDown:
                    // this node cannot connect to cluster, the task will continue on this node
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Backup task '{taskId}' state is '{taskState}', will continue to execute by the current node '{_database.ServerStore.NodeTag}'.");

                    return;
                case TaskStatus.ActiveByCurrentNode:
                    // a backup is already running, the next one will be re-scheduled by the backup task if needed
                    if (existingBackupState.RunningTask != null)
                    {
                        if (_logger.IsOperationsEnabled)
                            _logger.Operations($"Backup task '{taskId}' state is '{taskState}', and currently are being executed.");

                        return;
                    }

                    // backup frequency hasn't changed, and we have a scheduled backup
                    if (previousConfiguration.HasBackupFrequencyChanged(newConfiguration) == false && existingBackupState.HasScheduledBackup())
                    {
                        if (_logger.IsOperationsEnabled)
                            _logger.Operations($"Backup task '{taskId}' state is '{taskState}', the task doesn't have frequency changes and has scheduled backup, will continue to execute by the current node '{_database.ServerStore.NodeTag}'.");

                        return;
                    }

                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Backup task '{taskId}' state is '{taskState}', the task has frequency changes or doesn't have scheduled backup, the timer will be rearranged and the task will be executed by current node '{_database.ServerStore.NodeTag}'.");


                    existingBackupState.UpdateTimer(GetTimer(newConfiguration, backupStatus), lockTaken: false);
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
            ClusterDown
        }

        private TaskStatus GetTaskStatus(DatabaseTopology topology, PeriodicBackupConfiguration configuration, bool disableLog = false)
        {
            if (configuration.Disabled)
                return TaskStatus.Disabled;

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

                return TaskStatus.Disabled;
            }

            var backupStatus = GetBackupStatus(configuration.TaskId);
            var whoseTaskIsIt = _database.WhoseTaskIsIt(topology, configuration, backupStatus, keepTaskOnOriginalMemberNode: true);
            if (whoseTaskIsIt == null)
                return TaskStatus.ClusterDown;

            if (whoseTaskIsIt == _serverStore.NodeTag)
                return TaskStatus.ActiveByCurrentNode;

            if (disableLog == false && _logger.IsInfoEnabled)
                _logger.Info($"Backup job is skipped at {SystemTime.UtcNow}, because it is managed " +
                             $"by '{whoseTaskIsIt}' node and not the current node ({_serverStore.NodeTag})");

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

        public bool HasRunningBackups()
        {
            foreach (var periodicBackup in _periodicBackups)
            {
                var runningTask = periodicBackup.Value.RunningTask;
                if (runningTask != null &&
                    runningTask.Task.IsCompleted == false)
                    return true;
            }

            return false;
        }

        public BackupInfo GetBackupInfo()
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return GetBackupInfoInternal(context);
            }
        }

        public BackupInfo GetBackupInfo(TransactionOperationContext context)
        {
            return GetBackupInfoInternal(context);
        }

        private BackupInfo GetBackupInfoInternal(TransactionOperationContext context)
        {
            var oneTimeBackupStatus = GetBackupStatusFromCluster(_serverStore, context, _database.Name, taskId: 0L);

            if (_periodicBackups.Count == 0 && oneTimeBackupStatus == null)
                return null;

            var lastBackup = 0L;
            PeriodicBackupStatus lastBackupStatus = null;
            var intervalUntilNextBackupInSec = long.MaxValue;
            if (oneTimeBackupStatus?.LastFullBackup != null && oneTimeBackupStatus.LastFullBackup.Value.Ticks > lastBackup)
            {
                lastBackup = oneTimeBackupStatus.LastFullBackup.Value.Ticks;
                lastBackupStatus = oneTimeBackupStatus;
            }

            foreach (var periodicBackup in _periodicBackups)
            {
                var status = ComparePeriodicBackupStatus(periodicBackup.Value.Configuration.TaskId,
                    backupStatus: GetBackupStatusFromCluster(_serverStore, context, _database.Name, periodicBackup.Value.Configuration.TaskId),
                    inMemoryBackupStatus: periodicBackup.Value.BackupStatus);

                if (status.LastFullBackup != null && status.LastFullBackup.Value.Ticks > lastBackup)
                {
                    lastBackup = status.LastFullBackup.Value.Ticks;
                    lastBackupStatus = status;
                }

                if (status.LastIncrementalBackup != null && status.LastIncrementalBackup.Value.Ticks > lastBackup)
                {
                    lastBackup = status.LastIncrementalBackup.Value.Ticks;
                    lastBackupStatus = status;
                }

                var nextBackup = GetNextBackupDetails(periodicBackup.Value.Configuration, status, _serverStore.NodeTag, skipErrorLog: true);
                if (nextBackup == null)
                    continue;

                if (nextBackup.TimeSpan.Ticks < intervalUntilNextBackupInSec)
                    intervalUntilNextBackupInSec = nextBackup.TimeSpan.Ticks;
            }

            return new BackupInfo
            {
                LastBackup = lastBackup == 0L ? (DateTime?)null : new DateTime(lastBackup),
                IntervalUntilNextBackupInSec = intervalUntilNextBackupInSec == long.MaxValue ? 0 : new TimeSpan(intervalUntilNextBackupInSec).TotalSeconds,
                BackupTaskType = lastBackupStatus?.TaskId == 0 ? BackupTaskType.OneTime : BackupTaskType.Periodic,
                Destinations = AddDestinations(lastBackupStatus)
            };

            static List<string> AddDestinations(PeriodicBackupStatus backupStatus)
            {
                if (backupStatus == null)
                    return null;

                var destinations = new List<string>();
                if (backupStatus.UploadToAzure?.Skipped == false)
                    destinations.Add(nameof(BackupConfiguration.BackupDestination.Azure));
                if (backupStatus.UploadToGlacier?.Skipped == false)
                    destinations.Add(nameof(BackupConfiguration.BackupDestination.AmazonGlacier));
                if (backupStatus.UploadToFtp?.Skipped == false)
                    destinations.Add(nameof(BackupConfiguration.BackupDestination.FTP));
                if (backupStatus.UploadToGoogleCloud?.Skipped == false)
                    destinations.Add(nameof(BackupConfiguration.BackupDestination.GoogleCloud));
                if (backupStatus.UploadToS3?.Skipped == false)
                    destinations.Add(nameof(BackupConfiguration.BackupDestination.AmazonS3));
                if (backupStatus.LocalBackup?.TempFolderUsed == false)
                    destinations.Add(nameof(BackupConfiguration.BackupDestination.Local));
                if (destinations.Count == 0)
                    destinations.Add(nameof(BackupConfiguration.BackupDestination.None));

                return destinations;
            }
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
                return EmptyDictionary;

            return new Dictionary<string, long>
            {
                [Constants.Documents.Collections.AllDocumentsCollection] = minLastEtag,
                [Constants.TimeSeries.All] = minLastEtag
            };
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
            internal bool SimulateClusterDownStatus;
            internal bool ClusterDownStatusSimulated;
            internal bool SimulateActiveByOtherNodeStatus;
            internal bool SimulateFailedBackup;
        }
    }
}
