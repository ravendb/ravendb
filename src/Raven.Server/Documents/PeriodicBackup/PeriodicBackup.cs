using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class PeriodicBackup
    {
        private Timer _backupTimer;
        private readonly SemaphoreSlim _updateTimerSemaphore = new SemaphoreSlim(1);
        private readonly SemaphoreSlim _updateBackupTaskSemaphore = new SemaphoreSlim(1);

        public Task RunningTask { get; set; }

        public DateTime StartTime { get; set; }

        public PeriodicBackupConfiguration Configuration { get; set; }

        public PeriodicBackupStatus BackupStatus { get; set; }

        public PeriodicBackupStatus RunningBackupStatus { get; set; }

        public void DisableFutureBackups()
        {
            _updateTimerSemaphore.Wait();

            try
            {
                _backupTimer?.Dispose();
                _backupTimer = null;
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
                if (discardIfDisabled && _backupTimer == null)
                    return;

                _backupTimer?.Dispose();
                _backupTimer = newBackupTimer;
            }
            finally
            {
                _updateTimerSemaphore.Release();
            }
        }

        public void UpdateBackupTask(Action action)
        {
            if (_updateBackupTaskSemaphore.Wait(0) == false)
                return;

            try
            {
                action();
            }
            finally
            {
                _updateBackupTaskSemaphore.Release();
            }
        }

        public bool HasScheduledBackup()
        {
            return _backupTimer != null;
        }
    }
}
