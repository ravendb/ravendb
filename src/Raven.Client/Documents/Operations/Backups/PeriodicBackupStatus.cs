using System;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Backups
{
    public class PeriodicBackupStatus : IDatabaseTaskStatus
    {
        public long TaskId { get; set; }

        public BackupType BackupType { get; set; }

        public bool IsFull { get; set; }

        public string NodeTag { get; set; }

        public DateTime? LastFullBackup { get; set; }

        public DateTime? LastIncrementalBackup { get; set; }

        public DateTime? LastFullBackupInternal { get; set; }

        public DateTime? LastIncrementalBackupInternal { get; set; }

        public LocalBackup LocalBackup { get; set; }

        public UploadToS3 UploadToS3;

        public UploadToGlacier UploadToGlacier;

        public UploadToAzure UploadToAzure;

        public UploadToGoogleCloud UploadToGoogleCloud;

        public UploadToFtp UploadToFtp;

        public long? LastEtag { get; set; }

        public string LastDatabaseChangeVector { get; set; }

        public LastRaftIndex LastRaftIndex { get; set; }

        public string FolderName { get; set; }

        public long? DurationInMs { get; set; }

        public long? LocalRetentionDurationInMs { get; set; }

        public long Version { get; set; }

        public Error Error { get; set; }

        public long? LastOperationId { get; set; }

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
            json[nameof(LastFullBackupInternal)] = LastFullBackupInternal;
            json[nameof(LastIncrementalBackupInternal)] = LastIncrementalBackupInternal;
            json[nameof(LocalBackup)] = LocalBackup?.ToJson();
            json[nameof(UploadToS3)] = UploadToS3?.ToJson();
            json[nameof(UploadToGlacier)] = UploadToGlacier?.ToJson();
            json[nameof(UploadToAzure)] = UploadToAzure?.ToJson();
            json[nameof(UploadToGoogleCloud)] = UploadToGoogleCloud?.ToJson();
            json[nameof(UploadToFtp)] = UploadToFtp?.ToJson();
            json[nameof(LastEtag)] = LastEtag;
            json[nameof(LastRaftIndex)] = LastRaftIndex?.ToJson();
            json[nameof(FolderName)] = FolderName;
            json[nameof(DurationInMs)] = DurationInMs;
            json[nameof(LocalRetentionDurationInMs)] = LocalRetentionDurationInMs;
            json[nameof(Version)] = Version;
            json[nameof(Error)] = Error?.ToJson();
            json[nameof(LastOperationId)] = LastOperationId;
            json[nameof(LastDatabaseChangeVector)] = LastDatabaseChangeVector;
        }

        public static string Prefix => "periodic-backups/";

        public static string GenerateItemName(string databaseName, long taskId)
        {
            return $"values/{databaseName}/{Prefix}{taskId}";
        }
    }

    public class Error
    {
        public string Exception { get; set; }

        public DateTime At { get; set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Exception)] = Exception,
                [nameof(At)] = At
            };
        }
    }
}
