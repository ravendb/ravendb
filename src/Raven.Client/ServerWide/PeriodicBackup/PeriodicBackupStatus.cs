using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.PeriodicBackup
{
    public class PeriodicBackupStatus
    {
        public long TaskId { get; set; }

        public BackupType BackupType { get; set; }

        public bool IsFull { get; set; }

        public string NodeTag { get; set; }

        public DateTime? LastFullBackup { get; set; }

        public DateTime? LastIncrementalBackup { get; set; }

        public LocalBackup LocalBackup { get; set; }

        public UploadToS3 UploadToS3;

        public UploadToGlacier UploadToGlacier;

        public UploadToAzure UploadToAzure;

        public UploadToFtp UploadToFtp;

        public long? LastExportedEtag { get; set; }

        public Dictionary<string, long> LastEtagsByCollection { get; set; } = new Dictionary<string, long>();

        public string FolderName { get; set; }

        public long? DurationInMs { get; set; }

        public long Version { get; set; }

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
            json[nameof(IsFull)] = IsFull;
            json[nameof(NodeTag)] = NodeTag;
            json[nameof(LastFullBackup)] = LastFullBackup;
            json[nameof(LastIncrementalBackup)] = LastIncrementalBackup;
            json[nameof(LocalBackup)] = LocalBackup?.ToJson();
            json[nameof(UploadToS3)] = UploadToS3?.ToJson();
            json[nameof(UploadToGlacier)] = UploadToGlacier?.ToJson();
            json[nameof(UploadToAzure)] = UploadToAzure?.ToJson();
            json[nameof(LastExportedEtag)] = LastExportedEtag;
            json[nameof(FolderName)] = FolderName;
            json[nameof(DurationInMs)] = DurationInMs;
            json[nameof(Version)] = Version;
        }

        public static string GenerateItemName(string databaseName, long taskId)
        {
            return $"values/{databaseName}/periodic-backups/{taskId}";
        }
    }
}
