using Sparrow.Json.Parsing;

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

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Key)] = Key,
                [nameof(EncryptionMode)] = EncryptionMode
            };
        }
    }

    public enum EncryptionMode {
        None,
        UseDatabaseKey,
        UseProvidedKey
    }
}
