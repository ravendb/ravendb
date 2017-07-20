using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Server.PeriodicBackup;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class PeriodicBackup
    {
        public Timer BackupTimer { get; set; }

        public Task RunningTask { get; set; }

        public PeriodicBackupConfiguration Configuration { get; set; }

        public PeriodicBackupStatus BackupStatus { get; set; }

        public bool Disposed { get; set; }

        public void DisableFutureBackups()
        {
            Disposed = true;
            BackupTimer?.Dispose();
        }
    }
}