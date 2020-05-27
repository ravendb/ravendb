//-----------------------------------------------------------------------
// <copyright file="PeriodicBackupConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using Raven.Client.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Backups
{
    public class PeriodicBackupConfiguration : IDatabaseTask, IDynamicJsonValueConvertible
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
            var destinations = GetFullBackupDestinations();
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
            return GetBackupDestinations().Select(x => x.ToString()).ToList();
        }

        internal List<string> GetFullBackupDestinations()
        {
            // used for studio and generating the default task name
            return GetBackupDestinations().Select(backupDestination =>
            {
                var str = backupDestination.ToString();
                var fieldInfo = typeof(BackupDestination).GetField(str);
                var attributes = (DescriptionAttribute[])fieldInfo.GetCustomAttributes(typeof(DescriptionAttribute), false);
                return attributes.Length > 0 ? attributes[0].Description : str;
            }).ToList();
        }

        private List<BackupDestination> GetBackupDestinations()
        {
            var backupDestinations = new List<BackupDestination>();

            AddBackupDestination(LocalSettings, BackupDestination.Local);
            AddBackupDestination(S3Settings, BackupDestination.AmazonS3);
            AddBackupDestination(GlacierSettings, BackupDestination.AmazonGlacier);
            AddBackupDestination(AzureSettings, BackupDestination.Azure);
            AddBackupDestination(GoogleCloudSettings, BackupDestination.GoogleCloud);
            AddBackupDestination(FtpSettings, BackupDestination.FTP);

            void AddBackupDestination(BackupSettings backupSettings, BackupDestination backupDestination)
            {
                if (backupSettings == null || backupSettings.Disabled)
                    return;

                backupDestinations.Add(backupDestination);
            }

            return backupDestinations;
        }

        internal enum BackupDestination
        {
            None,
            Local,
            [Description("Amazon S3")]
            AmazonS3,
            [Description("Amazon Glacier")]
            AmazonGlacier,
            Azure,
            [Description("Google Cloud")]
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
