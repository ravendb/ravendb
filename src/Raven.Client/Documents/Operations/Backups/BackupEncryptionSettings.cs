namespace Raven.Client.Documents.Operations.Backups
{
    public class BackupEncryptionSettings
    {
        public string Key { get; set; }
        public EncryptionMode EncryptionMode { get; set; }
    }
    
    public enum EncryptionMode {
        None,
        UseDatabaseKey,
        UseProvidedKey
    }
}
