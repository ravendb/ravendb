using System;
using System.Linq;
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

        public DateTime? LastFullBackup
        {
            get
            {
                var lastLocalBackup = LocalBackupStatus?.LastFullBackup ?? DateTime.MinValue;
                var lastS3Backup = LocalBackupStatus?.LastFullBackup ?? DateTime.MinValue;
                var lastGlacierBackup = LocalBackupStatus?.LastFullBackup ?? DateTime.MinValue;
                var lastAzureBackup = LocalBackupStatus?.LastFullBackup ?? DateTime.MinValue;

                var minDate = new DateTime(new[]
                {
                    lastLocalBackup.Ticks,
                    lastS3Backup.Ticks,
                    lastGlacierBackup.Ticks,
                    lastAzureBackup.Ticks
                }.Min());

                return minDate == DateTime.MinValue ? (DateTime?)null : minDate;
            }
        }

        public DateTime? LastIncrementalBackup
        {
            get
            {
                var lastLocalBackup = LocalBackupStatus?.LastIncrementalBackup ?? DateTime.MinValue;
                var lastS3Backup = LocalBackupStatus?.LastIncrementalBackup ?? DateTime.MinValue;
                var lastGlacierBackup = LocalBackupStatus?.LastIncrementalBackup ?? DateTime.MinValue;
                var lastAzureBackup = LocalBackupStatus?.LastIncrementalBackup ?? DateTime.MinValue;

                var minDate = new DateTime(new[]
                {
                    lastLocalBackup.Ticks,
                    lastS3Backup.Ticks,
                    lastGlacierBackup.Ticks,
                    lastAzureBackup.Ticks
                }.Min());

                return minDate == DateTime.MinValue ? (DateTime?)null : minDate;
            }
        }

        public long? LastEtag { get; set; }

        public long? DurationInMs { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(BackupType)] = BackupType.ToString(),
                [nameof(LocalBackupStatus)] = LocalBackupStatus.ToJson(),
                [nameof(S3BackupStatus)] = S3BackupStatus.ToJson(),
                [nameof(GlacierBackupStatus)] = GlacierBackupStatus.ToJson(),
                [nameof(AzureBackupStatus)] = AzureBackupStatus.ToJson(),
                [nameof(LastEtag)] = LastEtag,
                [nameof(DurationInMs)] = DurationInMs
            };
        }
    }
}