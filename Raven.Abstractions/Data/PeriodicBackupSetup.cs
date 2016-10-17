//-----------------------------------------------------------------------
// <copyright file="BackupStatus.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Abstractions.Data
{
    public class PeriodicExportSetup
    {
        public const string RavenDocumentKey = "Raven/Backup/Periodic/Setup";

        /// <summary>
        /// Indicates if periodic export is disabled.
        /// </summary>
        public bool Disabled { get; set; }

        /// <summary>
        /// Amazon Glacier Vaul name.
        /// </summary>
        public string GlacierVaultName { get; set; }

        /// <summary>
        /// Amazon S3 Bucket name.
        /// </summary>
        public string S3BucketName { get; set; }

        /// <summary>
        /// Amazon Web Services (AWS) region.
        /// </summary>
        public string AwsRegionEndpoint { get; set; }

        /// <summary>
        /// Microsoft Azure Storage Container name.
        /// </summary>
        public string AzureStorageContainer { get; set; }

        /// <summary>
        /// Path to local folder. If not empty, backups will be held in this folder and not deleted. Otherwise, backups will be created in DataDir of a database and deleted after successful upload to Glacier/S3/Azure.
        /// </summary>
        public string LocalFolderName { get; set; }

        /// <summary>
        /// Path to remote azure folder.
        /// </summary>
        public string AzureRemoteFolderName { get; set; }

        /// <summary>
        /// Path to remote azure folder.
        /// </summary>
        public string S3RemoteFolderName { get; set; }

        /// <summary>
        /// Interval between incremental backups in milliseconds. If set to null or 0 then incremental periodic export will be disabled.
        /// </summary>
        public long? IntervalMilliseconds { get; set; }

        /// <summary>
        /// Interval between full backups in milliseconds. If set to null or 0 then full periodic export will be disabled.
        /// </summary>
        public long? FullBackupIntervalMilliseconds { get; set; }

        protected bool Equals(PeriodicExportSetup other)
        {
            return string.Equals(Disabled, other.Disabled) &&
                   string.Equals(GlacierVaultName, other.GlacierVaultName) && string.Equals(S3BucketName, other.S3BucketName) &&
                   string.Equals(AwsRegionEndpoint, other.AwsRegionEndpoint) &&
                   string.Equals(AzureStorageContainer, other.AzureStorageContainer) &&
                   string.Equals(LocalFolderName, other.LocalFolderName) && 
                   IntervalMilliseconds == other.IntervalMilliseconds && FullBackupIntervalMilliseconds == other.FullBackupIntervalMilliseconds;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((PeriodicExportSetup) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Disabled.GetHashCode();
                hashCode = (hashCode*397) ^ (GlacierVaultName != null ? GlacierVaultName.GetHashCode() : 0);
                hashCode = (hashCode*397) ^ (S3BucketName != null ? S3BucketName.GetHashCode() : 0);
                hashCode = (hashCode*397) ^ (AwsRegionEndpoint != null ? AwsRegionEndpoint.GetHashCode() : 0);
                hashCode = (hashCode*397) ^ (AzureStorageContainer != null ? AzureStorageContainer.GetHashCode() : 0);
                hashCode = (hashCode*397) ^ (LocalFolderName != null ? LocalFolderName.GetHashCode() : 0);
                hashCode = (hashCode*397) ^ (int)IntervalMilliseconds.GetValueOrDefault();
                hashCode = (hashCode*397) ^ (int)FullBackupIntervalMilliseconds.GetValueOrDefault();
                return hashCode;
            }
        }
    }

    public class PeriodicExportStatus
    {
        public const string RavenDocumentKey = "Raven/Backup/Periodic/Status";
        public DateTime LastBackup { get; set; }
        public DateTime LastFullBackup { get; set; }
        public Etag LastDocsEtag { get; set; }
        public string LastFullLocalBackupFolder { get; set; }

        [Obsolete("Use RavenFS instead.")]
        public Etag LastAttachmentsEtag { get; set; }

        public Etag LastDocsDeletionEtag { get; set; }

        [Obsolete("Use RavenFS instead.")]
        public Etag LastAttachmentDeletionEtag { get; set; }

        public PeriodicExportStatus()
        {
            LastDocsEtag = Etag.Empty;
            LastAttachmentsEtag = Etag.Empty;
            LastDocsDeletionEtag = Etag.Empty;
            LastAttachmentDeletionEtag = Etag.Empty;
        }
        [Flags]
        public enum PeriodicExportStatusEtags
        {
            None = 0,
            LastDocsEtag = 1,
            LastAttachmentsEtag = 2,
            LastDocsDeletionEtag = 4,
            LastAttachmentDeletionEtag = 8,
            All = 15
        }
    }
}
