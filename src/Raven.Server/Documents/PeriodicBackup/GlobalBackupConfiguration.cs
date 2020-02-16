using System.Threading;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class GlobalBackupConfiguration
    {
        public GlobalBackupConfiguration(int maxNumberOfConcurrentBackups)
        {
            MaxNumberOfConcurrentBackups = maxNumberOfConcurrentBackups;
            ConcurrentBackupsSemaphore = new SemaphoreSlim(maxNumberOfConcurrentBackups);
        }

        public SemaphoreSlim ConcurrentBackupsSemaphore;

        public int MaxNumberOfConcurrentBackups;
    }
}
