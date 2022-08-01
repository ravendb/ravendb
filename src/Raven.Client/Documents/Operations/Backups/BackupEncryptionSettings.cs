using System;
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

        internal BackupEncryptionSettings(BackupEncryptionSettings other)
        {
            if (other == null)
                throw new ArgumentException(nameof(other));

            Key = other.Key;
            EncryptionMode = other.EncryptionMode;
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
