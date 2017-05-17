//-----------------------------------------------------------------------
// <copyright file="PeriodicBackupConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Client.Documents;

namespace Raven.Client.Server.PeriodicBackup
{
    public class PeriodicBackupConfiguration: IDatabaseTask
    {
        public long? TaskId { get; set; }

        public bool Disabled { get; set; }

        public BackupType Type { get; set; }

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
            //TODO: check this
            return (ulong)TaskId.Value;
        }
    }
}