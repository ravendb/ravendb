namespace Raven.Client.Documents.Operations.Backups
{
    public class BackupEncryptionSettings
    {
        public string Key { get; set; }
        public EncryptionMode EncryptionMode { get; set; }
        public bool AllowUnencryptedBackupForEncryptedDatabase { get; set; }

        public BackupEncryptionSettings()
        {
            Key = null;
            EncryptionMode = EncryptionMode.None;
            AllowUnencryptedBackupForEncryptedDatabase = false;
        }
    }

    public enum EncryptionMode {
        None,
        UseDatabaseKey,
        UseProvidedKey
    }
}
