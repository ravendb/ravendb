using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Util;
using Raven.Server.ServerWide;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.Threading;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class PeriodicBackup : IDisposable
    {
        private readonly SemaphoreSlim _updateBackupTaskSemaphore = new SemaphoreSlim(1);
        private readonly DisposeOnce<SingleAttempt> _disposeOnce;
        private readonly PeriodicBackupRunner _periodicBackupRunner;
        private readonly Logger _logger;
        private BackupTimer _backupTimer;

        public RunningBackupTask RunningTask { get; set; }

        public OperationCancelToken CancelToken { get; set; }

        public DateTime StartTimeInUtc { get; set; }

        public PeriodicBackupConfiguration Configuration { get; set; }

        public PeriodicBackupStatus BackupStatus { get; set; }

        public PeriodicBackupStatus RunningBackupStatus { get; set; }

        public bool Disposed => _disposeOnce.Disposed;

        public PeriodicBackup(PeriodicBackupRunner periodicBackupRunner, ConcurrentSet<Task> inactiveRunningPeriodicBackupsTasks, Logger logger)
        {
            _periodicBackupRunner = periodicBackupRunner;
            _logger = logger;

            _disposeOnce = new DisposeOnce<SingleAttempt>(() =>
            {
                using (UpdateBackupTask())
                {
                    CancelFutureTasks();

                    var runningTask = RunningTask;
                    if (runningTask != null && runningTask.Task.IsCompleted == false)
                    {
                        inactiveRunningPeriodicBackupsTasks.Add(runningTask.Task);
                    }
                }
            });
        }

        public IDisposable UpdateBackupTask()
        {
            _updateBackupTaskSemaphore.Wait();

            return new DisposableAction(() => _updateBackupTaskSemaphore.Release());
        }

        public void DisableFutureBackups()
        {
            using (UpdateBackupTask())
            {
                CancelFutureTasks();
            }
        }

        private void CancelFutureTasks()
        {
            _backupTimer?.Dispose();
            _backupTimer = null;

            try
            {
                CancelToken?.Cancel();
            }
            catch
            {
            }
        }

        public void UpdateTimer(NextBackup nextBackup, bool lockTaken, bool discardIfDisabled = false)
        {
            if (nextBackup == null)
                return;

            if (lockTaken == false)
            {
                using (UpdateBackupTask())
                {
                    UpdateTimerInternal(nextBackup, discardIfDisabled);
                }
            }
            else
            {
                UpdateTimerInternal(nextBackup, discardIfDisabled);
            }
        }

        private void UpdateTimerInternal(NextBackup nextBackup, bool discardIfDisabled)
        {
            if (Disposed)
                return;

            if (discardIfDisabled && _backupTimer == null)
                return;

            _backupTimer?.Dispose();

            if (_logger.IsOperationsEnabled)
                _logger.Operations($"Next {(nextBackup.IsFull ? "full" : "incremental")} backup is in {nextBackup.TimeSpan.TotalMinutes} minutes.");

            var timer = nextBackup.TimeSpan < _periodicBackupRunner.MaxTimerTimeout
                ? new Timer(_periodicBackupRunner.TimerCallback, nextBackup, nextBackup.TimeSpan, Timeout.InfiniteTimeSpan)
                : new Timer(_periodicBackupRunner.LongPeriodTimerCallback, nextBackup, _periodicBackupRunner.MaxTimerTimeout, Timeout.InfiniteTimeSpan);
            
            _backupTimer = new BackupTimer
            {
                Timer = timer,
                CreatedAt = DateTime.UtcNow,
                NextBackup = nextBackup
            };
        }

        public bool HasScheduledBackup()
        {
            return _backupTimer != null;
        }

        internal Timer GetTimer()
        {
            return _backupTimer?.Timer;
        }

        internal NextBackup GetNextBackup()
        {
            return _backupTimer?.NextBackup;
        }

        internal DateTime? GetCreatedAt()
        {
            return _backupTimer?.CreatedAt;
        }

        public void Dispose()
        {
            _disposeOnce.Dispose();
        }

        public class RunningBackupTask
        {
            public Task Task { get; set; }

            public long Id { get; set; }
        }

        public class BackupTimer : IDisposable
        {
            public Timer Timer { get; set; }

            public NextBackup NextBackup { get; set; }

            public DateTime CreatedAt { get; set; }

            public void Dispose()
            {
                Timer?.Dispose();
            }
        }
    }
}
