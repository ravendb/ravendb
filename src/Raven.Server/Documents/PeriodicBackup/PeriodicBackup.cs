using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class PeriodicBackup
    {
        private readonly SemaphoreSlim _updateTimerSemaphore = new SemaphoreSlim(1);
        public readonly SemaphoreSlim UpdateBackupTaskSemaphore = new SemaphoreSlim(1);

        public Timer BackupTimer { get; private set; }

        public Task RunningTask { get; set; }

        public long? RunningBackupTaskId { get; set; }

        public OperationCancelToken CancelToken { get; set; }

        public DateTime StartTime { get; set; }

        public PeriodicBackupConfiguration Configuration { get; set; }

        public PeriodicBackupStatus BackupStatus { get; set; }

        public PeriodicBackupStatus RunningBackupStatus { get; set; }

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
    }
}
