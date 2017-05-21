using System;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.Server.PeriodicBackup
{
    public class PeriodicBackupStatus
    {
        public long TaskId { get; set; }

        public BackupType BackupType { get; set; }

        public string NodeTag { get; set; }

        public LocalBackup LocalBackup { get; set; }

        public UploadToS3 UploadToS3 { get; set; }

        public UploadToGlacier UploadToGlacier { get; set; }

        public UploadToAzure UploadToAzure { get; set; }

        public DateTime? LastFullBackup
        {
            get
            {
                var lastLocalBackup = LocalBackup?.LastFullBackup ?? DateTime.MinValue;
                var lastS3Backup = LocalBackup?.LastFullBackup ?? DateTime.MinValue;
                var lastGlacierBackup = LocalBackup?.LastFullBackup ?? DateTime.MinValue;
                var lastAzureBackup = LocalBackup?.LastFullBackup ?? DateTime.MinValue;

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
                var lastLocalBackup = LocalBackup?.LastIncrementalBackup ?? DateTime.MinValue;
                var lastS3Backup = LocalBackup?.LastIncrementalBackup ?? DateTime.MinValue;
                var lastGlacierBackup = LocalBackup?.LastIncrementalBackup ?? DateTime.MinValue;
                var lastAzureBackup = LocalBackup?.LastIncrementalBackup ?? DateTime.MinValue;

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
            var json = new DynamicJsonValue();
            UpdateJson(json);
            return json;
        }

        public void UpdateJson(DynamicJsonValue json)
        {
            json[nameof(TaskId)] = TaskId;
            json[nameof(BackupType)] = BackupType;
            json[nameof(NodeTag)] = NodeTag;
            json[nameof(LocalBackup)] = LocalBackup?.ToJson();
            json[nameof(UploadToS3)] = UploadToS3?.ToJson();
            json[nameof(UploadToGlacier)] = UploadToGlacier?.ToJson();
            json[nameof(UploadToAzure)] = UploadToAzure?.ToJson();
            json[nameof(LastEtag)] = LastEtag;
            json[nameof(DurationInMs)] = DurationInMs;
        }

        public static string GenerateItemName(string databaseName, long taskId)
        {
            return $"periodic-backups/{databaseName}/{taskId}";
        }
    }
}