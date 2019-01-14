namespace Raven.Client.Documents.Operations.Backups
{
    public class BackupEncryptionSettings
    {
        public string Key { get; set; }
        public EncryptionMode EncryptionMode { get; set; }

        public BackupEncryptionSettings()
        {
            Key = null;
            EncryptionMode = EncryptionMode.None;
        }
    }

    public enum EncryptionMode {
        None,
        UseDatabaseKey,
        UseProvidedKey
    }
}
