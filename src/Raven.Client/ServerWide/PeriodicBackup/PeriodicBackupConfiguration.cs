//-----------------------------------------------------------------------
// <copyright file="PeriodicBackupConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Diagnostics;

namespace Raven.Client.ServerWide.PeriodicBackup
{
    public class PeriodicBackupConfiguration : IDatabaseTask
    {
        public long TaskId { get; set; }

        public bool Disabled { get; set; }

        public string Name { get; set; }

        public BackupType BackupType { get; set; }

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

        public bool Equals(PeriodicBackupConfiguration other)
        {
            if (other == null)
                return false;

            if (Equals(other.FullBackupFrequency, FullBackupFrequency) == false)
                return false;

            if (Equals(other.IncrementalBackupFrequency, IncrementalBackupFrequency) == false)
                return false;

            if (other.BackupType.Equals(BackupType) == false)
                return false;

            if (Equals(other.LocalSettings, LocalSettings) == false)
                return false;

            if (Equals(other.S3Settings, S3Settings) == false)
                return false;

            if (Equals(other.GlacierSettings, GlacierSettings) == false)
                return false;

            if (Equals(other.AzureSettings, AzureSettings) == false)
                return false;

            return true;
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
    }
}
