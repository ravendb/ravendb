//-----------------------------------------------------------------------
// <copyright file="BackupStatus.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Abstractions.Data
{
	public class PeriodicBackupSetup
	{
		public const string RavenDocumentKey = "Raven/Backup/Periodic/Setup";

		public string GlacierVaultName { get; set; }
		public string S3BucketName { get; set; }
		public string AwsRegionEndpoint { get; set; }
        public string AzureStorageContainer { get; set; }

		public string LocalFolderName { get; set; }

		public int IntervalMilliseconds { get; set; }

        public int FullBackupIntervalMilliseconds { get; set; }

		protected bool Equals(PeriodicBackupSetup other)
		{
			return string.Equals(GlacierVaultName, other.GlacierVaultName) && string.Equals(S3BucketName, other.S3BucketName) &&
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
			return Equals((PeriodicBackupSetup) obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				var hashCode = (GlacierVaultName != null ? GlacierVaultName.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (S3BucketName != null ? S3BucketName.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (AwsRegionEndpoint != null ? AwsRegionEndpoint.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (AzureStorageContainer != null ? AzureStorageContainer.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (LocalFolderName != null ? LocalFolderName.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ IntervalMilliseconds;
			    hashCode = (hashCode*397) ^ FullBackupIntervalMilliseconds;
				return hashCode;
			}
		}
	}

	public class PeriodicBackupStatus
	{
		public const string RavenDocumentKey = "Raven/Backup/Periodic/Status";
		public DateTime LastBackup { get; set; }
        public DateTime LastFullBackup { get; set; }
		public Etag LastDocsEtag { get; set; }
		public Etag LastAttachmentsEtag { get; set; }
        public Etag LastDocsDeletionEtag { get; set; }
        public Etag LastAttachmentDeletionEtag { get; set; }

		public PeriodicBackupStatus()
		{
			LastDocsEtag = Etag.Empty;
			LastAttachmentsEtag = Etag.Empty;
		    LastDocsDeletionEtag = Etag.Empty;
		    LastAttachmentDeletionEtag = Etag.Empty;
		}
	}
}
