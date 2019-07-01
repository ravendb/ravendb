using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Backups
{
    public abstract class RestoreBackupConfigurationBase : IDynamicJson
    {
        public string DatabaseName { get; set; }

        public string LastFileNameToRestore { get; set; }

        public string DataDirectory { get; set; }

        public string EncryptionKey { get; set; }

        public bool DisableOngoingTasks { get; set; }

        public bool SkipIndexes { get; set; }

        protected abstract RestoreType Type { get; }

        public BackupEncryptionSettings BackupEncryptionSettings { get; set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DatabaseName)] = DatabaseName,
                [nameof(LastFileNameToRestore)] = LastFileNameToRestore,
                [nameof(DataDirectory)] = DataDirectory,
                [nameof(EncryptionKey)] = EncryptionKey,
                [nameof(DisableOngoingTasks)] = DisableOngoingTasks,
                [nameof(SkipIndexes)] = SkipIndexes,
                [nameof(BackupEncryptionSettings)] = BackupEncryptionSettings,
                [nameof(Type)] = Type
            };
        }
    }

    public class RestoreBackupConfiguration : RestoreBackupConfigurationBase
    {
        public string BackupLocation { get; set; }

        protected override RestoreType Type => RestoreType.Local;

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(BackupLocation)] = BackupLocation;
            return json;
        }
    }

    public class RestoreFromS3Configuration : RestoreBackupConfigurationBase
    {
        public S3Settings Settings { get; set; } = new S3Settings();

        protected override RestoreType Type => RestoreType.S3;

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Settings)] = Settings;
            return json;
        }
    }

    public enum RestoreType
    {
        Local,
        S3
    }
}
