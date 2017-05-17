using System;

namespace Raven.Client.Server.PeriodicBackup
{
    public abstract class BackupSettings
    {
        public bool Disabled { get; set; }

        public abstract bool HasSettings();
    }

    public class LocalSettings : BackupSettings
    {
        /// <summary>
        /// Path to local folder. If not empty, exports will be held in this folder and not deleted. 
        /// Otherwise, backups will be created in DataDir of a database and deleted after successful upload to Glacier/S3/Azure.
        /// </summary>
        public string FolderPath { get; set; }

        public override bool HasSettings()
        {
            return string.IsNullOrWhiteSpace(FolderPath) == false;
        }
    }

    public abstract class AmazonSettings : BackupSettings
    {
        //TODO: should be encrypted
        public string AwsAccessKey { get; set; }

        //TODO: should be encrypted
        public string AwsSecretKey { get; set; }

        /// <summary>
        /// Amazon Web Services (AWS) region.
        /// </summary>
        public string AwsRegionName { get; set; }
    }

    public class S3Settings : AmazonSettings
    {
        /// <summary>
        /// Amazon S3 Bucket name.
        /// </summary>
        public string BucketName { get; set; }

        /// <summary>
        /// Amazon S3 Remote folder name.
        /// </summary>
        public string RemoteFolderName { get; set; }

        public override bool HasSettings()
        {
            return string.IsNullOrWhiteSpace(BucketName) == false;
        }
    }

    public class GlacierSettings : AmazonSettings
    {
        /// <summary>
        /// Amazon Glacier Vault name.
        /// </summary>
        public string VaultName { get; set; }

        public override bool HasSettings()
        {
            return string.IsNullOrWhiteSpace(VaultName) == false;
        }
    }

    public class AzureSettings : BackupSettings
    {
        /// <summary>
        /// Microsoft Azure Storage Container name.
        /// </summary>
        public string StorageContainer { get; set; }

        /// <summary>
        /// Path to remote azure folder.
        /// </summary>
        public string RemoteFolderName { get; set; }

        //TODO: should be encrypted
        public string StorageAccount { get; set; }

        //TODO: should be encrypted
        public string StorageKey { get; set; }

        public override bool HasSettings()
        {
            return string.IsNullOrWhiteSpace(StorageContainer) == false;
        }
    }
}