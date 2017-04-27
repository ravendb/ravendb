//-----------------------------------------------------------------------
// <copyright file="PeriodicBackup.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Client.Server.PeriodicExport
{
    public class PeriodicBackupConfiguration
    {
        /// <summary>
        /// Indicates if periodic export is active.
        /// </summary>
        public bool Active { get; set; }

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
        public string AwsRegionName { get; set; }

        /// <summary>
        /// Microsoft Azure Storage Container name.
        /// </summary>
        public string AzureStorageContainer { get; set; }

        /// <summary>
        /// Path to local folder. If not empty, exports will be held in this folder and not deleted. Otherwise, exports will be created in DataDir of a database and deleted after successful upload to Glacier/S3/Azure.
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
        /// Interval between incremental exports in milliseconds. If set to null or 0 then incremental periodic export will be disabled.
        /// </summary>
        public long? IntervalMilliseconds { get; set; }

        /// <summary>
        /// Interval between full exports in milliseconds. If set to null or 0 then full periodic export will be disabled.
        /// </summary>
        public long? FullExportIntervalMilliseconds { get; set; }

        protected bool Equals(PeriodicBackupConfiguration other)
        {
            return Active == other.Active 
                && string.Equals(GlacierVaultName, other.GlacierVaultName) 
                && string.Equals(S3BucketName, other.S3BucketName) 
                && string.Equals(AwsRegionName, other.AwsRegionName) 
                && string.Equals(AzureStorageContainer, other.AzureStorageContainer) 
                && string.Equals(LocalFolderName, other.LocalFolderName) 
                && string.Equals(AzureRemoteFolderName, other.AzureRemoteFolderName) 
                && string.Equals(S3RemoteFolderName, other.S3RemoteFolderName) 
                && IntervalMilliseconds == other.IntervalMilliseconds 
                && FullExportIntervalMilliseconds == other.FullExportIntervalMilliseconds;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((PeriodicBackupConfiguration)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Active.GetHashCode();
                hashCode = (hashCode * 397) ^ (GlacierVaultName != null ? GlacierVaultName.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (S3BucketName != null ? S3BucketName.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (AwsRegionName != null ? AwsRegionName.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (AzureStorageContainer != null ? AzureStorageContainer.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (LocalFolderName != null ? LocalFolderName.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (AzureRemoteFolderName != null ? AzureRemoteFolderName.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (S3RemoteFolderName != null ? S3RemoteFolderName.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ IntervalMilliseconds.GetHashCode();
                hashCode = (hashCode * 397) ^ FullExportIntervalMilliseconds.GetHashCode();
                return hashCode;
            }
        }
    }
}