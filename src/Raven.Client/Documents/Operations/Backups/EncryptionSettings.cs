namespace Raven.Client.Documents.Operations.Backups
{
    public class EncryptionSettings
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
