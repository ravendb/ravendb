using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.ServerWide;
using Sparrow.Collections;
using Sparrow.Threading;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class PeriodicBackup : IDisposable
    {
        private readonly SemaphoreSlim _updateTimerSemaphore = new SemaphoreSlim(1);
        public readonly SemaphoreSlim UpdateBackupTaskSemaphore = new SemaphoreSlim(1);

        private readonly DisposeOnce<SingleAttempt> _disposeOnce;

        public Timer BackupTimer { get; private set; }

        public Task RunningTask { get; set; }

        public long? RunningBackupTaskId { get; set; }

        public OperationCancelToken CancelToken { get; set; }

        public DateTime StartTime { get; set; }

        public PeriodicBackupConfiguration Configuration { get; set; }

        public PeriodicBackupStatus BackupStatus { get; set; }

        public PeriodicBackupStatus RunningBackupStatus { get; set; }
        
        public PeriodicBackup(ConcurrentSet<Task> inactiveRunningPeriodicBackupsTasks)
        {
            _disposeOnce = new DisposeOnce<SingleAttempt>(() =>
            {
                DisableFutureBackups();

                if (RunningTask?.IsCompleted == false)
                {
                    inactiveRunningPeriodicBackupsTasks.Add(RunningTask);
                }
            });
        }

        public void DisableFutureBackups()
        {
            _updateTimerSemaphore.Wait();

            try
            {
                BackupTimer?.Dispose();
                BackupTimer = null;

                try
                {
                    CancelToken?.Cancel();
                }
                catch {}
            }
            finally
            {
                _updateTimerSemaphore.Release();
            }
        }

        public void UpdateTimer(Timer newBackupTimer, bool discardIfDisabled = false)
        {
            _updateTimerSemaphore.Wait();

            try
            {
                if (discardIfDisabled && BackupTimer == null)
                    return;

                BackupTimer?.Dispose();
                BackupTimer = newBackupTimer;
            }
            finally
            {
                _updateTimerSemaphore.Release();
            }
        }

        public bool HasScheduledBackup()
        {
            return BackupTimer != null;
        }

        public void Dispose()
        {
            _disposeOnce.Dispose();
        }
    }
}
