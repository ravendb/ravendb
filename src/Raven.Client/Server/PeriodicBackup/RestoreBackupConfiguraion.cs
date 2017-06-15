namespace Raven.Client.Server.PeriodicBackup
{
    public class RestoreBackupConfiguraion
    {
        public string BackupLocation { get; set; }

        public string DataDirectory { get; set; }

        public string DatabaseName { get; set; }

        public string JournalsStoragePath { get; set; }

        public string IndexingStoragePath { get; set; }

        public string TempPath { get; set; }
    }
}