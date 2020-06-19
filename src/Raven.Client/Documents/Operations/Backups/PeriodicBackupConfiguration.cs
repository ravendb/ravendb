//-----------------------------------------------------------------------
// <copyright file="PeriodicBackupConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Backups
{
    public class PeriodicBackupConfiguration : IDatabaseTask
    {
        public long TaskId { get; set; }
        public bool Disabled { get; set; }
        public string Name { get; set; }
        public string MentorNode { get; set; }
        public BackupType BackupType { get; set; }
        public BackupEncryptionSettings BackupEncryptionSettings { get; set; }
        public RetentionPolicy RetentionPolicy { get; set; }
        public SnapshotSettings SnapshotSettings { get; set; }

        public LocalSettings LocalSettings { get; set; }
        public S3Settings S3Settings { get; set; }
        public GlacierSettings GlacierSettings { get; set; }
        public AzureSettings AzureSettings { get; set; }
        public FtpSettings FtpSettings { get; set; }
        public GoogleCloudSettings GoogleCloudSettings { get; set; }

        /// <summary>
        /// Frequency of full backup jobs in cron format
        /// </summary>
        public string FullBackupFrequency { get; set; }

        /// <summary>
        /// Frequency of incremental backup jobs in cron format
        /// If set to null incremental backup will be disabled.
        /// </summary>
        public string IncrementalBackupFrequency { get; set; }

        public ulong GetTaskKey()
        {
            Debug.Assert(TaskId != 0);

            return (ulong)TaskId;
        }

        public string GetMentorNode()
        {
            return MentorNode;
        }

        public string GetDefaultTaskName()
        {
            var destinations = GetDestinations();
            return destinations.Count == 0 ?
                $"{BackupType} w/o destinations" :
                $"{BackupType} to {string.Join(", ", destinations)}";
        }

        public string GetTaskName()
        {
            return Name;
        }

        public bool IsResourceIntensive()
        {
            return true;
        }

        public bool HasBackupFrequencyChanged(PeriodicBackupConfiguration other)
        {
            if (other == null)
                return true;

            if (Equals(other.FullBackupFrequency, FullBackupFrequency) == false)
                return true;

            if (Equals(other.IncrementalBackupFrequency, IncrementalBackupFrequency) == false)
                return true;

            return false;
        }

        public bool HasBackup()
        {
            return CanBackupUsing(LocalSettings) ||
                   CanBackupUsing(S3Settings) ||
                   CanBackupUsing(GlacierSettings) ||
                   CanBackupUsing(AzureSettings) ||
                   CanBackupUsing(GoogleCloudSettings) ||
                   CanBackupUsing(FtpSettings);
        }

        public bool HasCloudBackup()
        {
            return CanBackupUsing(S3Settings) ||
                   CanBackupUsing(GlacierSettings) ||
                   CanBackupUsing(AzureSettings) ||
                   CanBackupUsing(GoogleCloudSettings) ||
                   CanBackupUsing(FtpSettings);
        }

        public static bool CanBackupUsing(BackupSettings settings)
        {
            return settings != null &&
                   settings.Disabled == false &&
                   settings.HasSettings();
        }

        public List<string> GetDestinations()
        {
            var backupDestinations = new List<string>();

            if (LocalSettings != null && LocalSettings.Disabled == false)
                backupDestinations.Add(nameof(BackupDestination.Local));
            if (AzureSettings != null && AzureSettings.Disabled == false)
                backupDestinations.Add(nameof(BackupDestination.Azure));
            if (S3Settings != null && S3Settings.Disabled == false)
                backupDestinations.Add(nameof(BackupDestination.AmazonS3));
            if (GlacierSettings != null && GlacierSettings.Disabled == false)
                backupDestinations.Add(nameof(BackupDestination.Glacier));
            if (GoogleCloudSettings != null && GoogleCloudSettings.Disabled == false)
                backupDestinations.Add(nameof(BackupDestination.GoogleCloud));
            if (FtpSettings != null && FtpSettings.Disabled == false)
                backupDestinations.Add(nameof(BackupDestination.FTP));

            return backupDestinations;
        }

        internal enum BackupDestination
        {
            None,
            Local,
            Azure,
            AmazonS3,
            Glacier,
            GoogleCloud,
            FTP
        }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TaskId)] = TaskId,
                [nameof(Disabled)] = Disabled,
                [nameof(Name)] = Name,
                [nameof(MentorNode)] = MentorNode,
                [nameof(BackupType)] = BackupType,
                [nameof(BackupEncryptionSettings)] = BackupEncryptionSettings?.ToJson(),
                [nameof(RetentionPolicy)] = RetentionPolicy?.ToJson(),
                [nameof(SnapshotSettings)] = SnapshotSettings?.ToJson(),
                [nameof(FullBackupFrequency)] = FullBackupFrequency,
                [nameof(IncrementalBackupFrequency)] = IncrementalBackupFrequency,
                [nameof(LocalSettings)] = LocalSettings?.ToJson(),
                [nameof(S3Settings)] = S3Settings?.ToJson(),
                [nameof(GlacierSettings)] = GlacierSettings?.ToJson(),
                [nameof(AzureSettings)] = AzureSettings?.ToJson(),
                [nameof(GoogleCloudSettings)] = GoogleCloudSettings?.ToJson(),
                [nameof(FtpSettings)] = FtpSettings?.ToJson()
            };
        }
    }
}
