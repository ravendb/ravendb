namespace Raven.Client.Documents.Operations.Backups
{
    public class RestoreBackupConfiguration
    {
        public string DatabaseName { get; set; }

        public string BackupLocation { get; set; }

        public string LastFileNameToRestore { get; set; }

        public string DataDirectory { get; set; }

        public string EncryptionKey { get; set; }

        public bool DisableOngoingTasks { get; set; }

        public bool SkipIndexes { get; set; }

        public EncryptionSettings EncryptionSettings { get; set; }
    }
}
