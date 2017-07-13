//-----------------------------------------------------------------------
// <copyright file="PeriodicBackupConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Diagnostics;

namespace Raven.Client.Server.PeriodicBackup
{
    public class PeriodicBackupConfiguration: IDatabaseTask
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
        
        public ulong GetTaskKey()
        {
            Debug.Assert(TaskId != 0);

            return (ulong)TaskId;
        }

        public bool Equals(PeriodicBackupConfiguration other)
        {
            if (other == null)
                return false;

            if (other.FullBackupFrequency.Equals(FullBackupFrequency) == false)
                return false;

            if (other.IncrementalBackupFrequency.Equals(IncrementalBackupFrequency) == false)
                return false;

            if (other.BackupType.Equals(BackupType) == false)
                return false;

            if (other.LocalSettings != null && other.LocalSettings.Equals(LocalSettings) == false)
                return false;

            if (other.S3Settings != null && other.S3Settings.Equals(S3Settings) == false)
                return false;

            if (other.GlacierSettings != null && other.GlacierSettings.Equals(GlacierSettings) == false)
                return false;

            if (other.AzureSettings != null && other.AzureSettings.Equals(AzureSettings) == false)
                return false;

            return true;
        }
    }
}