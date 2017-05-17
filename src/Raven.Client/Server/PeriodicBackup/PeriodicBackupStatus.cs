using Sparrow.Json.Parsing;

namespace Raven.Client.Server.PeriodicBackup
{
    public class PeriodicBackupStatus
    {
        public BackupType BackupType { get; set; }

        public string NodeTag { get; set; }

        public LocalBackupStatus LocalBackupStatus { get; set; }

        public S3BackupStatus S3BackupStatus { get; set; }

        public GlacierBackupStatus GlacierBackupStatus { get; set; }

        public AzureBackupStatus AzureBackupStatus { get; set; }

        public long? LastEtag { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(BackupType)] = BackupType.ToString(),
                [nameof(LocalBackupStatus)] = LocalBackupStatus.ToJson(),
                [nameof(S3BackupStatus)] = S3BackupStatus.ToJson(),
                [nameof(GlacierBackupStatus)] = GlacierBackupStatus.ToJson(),
                [nameof(AzureBackupStatus)] = AzureBackupStatus.ToJson(),
                [nameof(LastEtag)] = LastEtag
            };
        }
    }
}