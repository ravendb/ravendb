namespace Raven.Client.Documents.Operations.Backups
{
    public class RestoreBackupConfigurationBase
    {
        public string DatabaseName { get; set; }

        public string LastFileNameToRestore { get; set; }

        public string DataDirectory { get; set; }

        public string EncryptionKey { get; set; }

        public bool DisableOngoingTasks { get; set; }

        public bool SkipIndexes { get; set; }

        public BackupEncryptionSettings BackupEncryptionSettings { get; set; }
    }

    public class RestoreBackupConfiguration : RestoreBackupConfigurationBase
    {
        public string BackupLocation { get; set; }
    }

    public class RestoreFromS3Configuration : RestoreBackupConfigurationBase
    {
        public RestoreFromS3Configuration()
        {
            S3Settings = new S3Settings();
        }

        public S3Settings S3Settings { get; set; }
    }
}
