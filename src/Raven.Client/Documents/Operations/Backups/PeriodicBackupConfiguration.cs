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

        /// <summary>
        /// Frequency of full backup jobs in cron format
        /// </summary>
        public string FullBackupFrequency { get; set; }

        /// <summary>
        /// Frequency of incremental backup jobs in cron format
        /// If set to null incremental backup will be disabled.
        /// </summary>
        public string IncrementalBackupFrequency { get; set; }

        public LocalSettings LocalSettings { get; set; }

        public S3Settings S3Settings { get; set; }

        public GlacierSettings GlacierSettings { get; set; }

        public AzureSettings AzureSettings { get; set; }

        public FtpSettings FtpSettings { get; set; }

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
                $"{BackupType.ToString()} to {string.Join(", ", destinations)}";
        }

        public string GetTaskName()
        {
            return Name;
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
                   CanBackupUsing(FtpSettings);
        }

        public bool HasCloudBackup()
        {
            return CanBackupUsing(S3Settings) ||
                   CanBackupUsing(GlacierSettings) ||
                   CanBackupUsing(AzureSettings) ||
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
                backupDestinations.Add("Local");
            if (AzureSettings != null && AzureSettings.Disabled == false)
                backupDestinations.Add("Azure");
            if (S3Settings != null && S3Settings.Disabled == false)
                backupDestinations.Add("S3");
            if (GlacierSettings != null && GlacierSettings.Disabled == false)
                backupDestinations.Add("Glacier");
            if (FtpSettings != null && FtpSettings.Disabled == false)
                backupDestinations.Add("FTP");

            return backupDestinations;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TaskId)] = TaskId,
                [nameof(Disabled)] = Disabled,
                [nameof(Name)] = Name,
                [nameof(MentorNode)] = MentorNode,
                [nameof(BackupType)] = BackupType,
                [nameof(BackupEncryptionSettings)] = BackupEncryptionSettings?.ToJson(),
                [nameof(FullBackupFrequency)] = FullBackupFrequency,
                [nameof(IncrementalBackupFrequency)] = IncrementalBackupFrequency,
                [nameof(LocalSettings)] = LocalSettings?.ToJson(),
                [nameof(S3Settings)] = S3Settings?.ToJson(),
                [nameof(GlacierSettings)] = GlacierSettings?.ToJson(),
                [nameof(AzureSettings)] = AzureSettings?.ToJson(),
                [nameof(FtpSettings)] = FtpSettings?.ToJson()
            };
        }
    }
}
