namespace Raven.Client.Server.PeriodicBackup
{
    public abstract class BackupSettings
    {
        public bool Disabled { get; set; }

        public abstract bool HasSettings();

        public virtual bool WasEnabled(BackupSettings other)
        {
            return Disabled && other.Disabled == false;
        }
    }

    public class LocalSettings : BackupSettings
    {
        /// <summary>
        /// Path to local folder. If not empty, backups will be held in this folder and not deleted. 
        /// Otherwise, backups will be created in the TempDir of a database and deleted after successful upload to S3/Glacier/Azure.
        /// </summary>
        public string FolderPath { get; set; }

        public override bool HasSettings()
        {
            return string.IsNullOrWhiteSpace(FolderPath) == false;
        }

        public bool Equals(LocalSettings other)
        {
            if (other == null)
                return false;

            if (WasEnabled(other))
                return true;

            return other.FolderPath.Equals(FolderPath);
        }
    }

    public abstract class AmazonSettings : BackupSettings
    {
        public string AwsAccessKey { get; set; }

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

        public bool Equals(S3Settings other)
        {
            if (other == null)
                return false;

            if (WasEnabled(other))
                return true;

            if (other.AwsRegionName != AwsRegionName)
                return false;

            if (other.BucketName != BucketName)
                return false;

            if (other.RemoteFolderName != RemoteFolderName)
                return false;

            return true;
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

        public bool Equals(GlacierSettings other)
        {
            if (other == null)
                return false;

            if (WasEnabled(other))
                return true;

            if (other.AwsRegionName != AwsRegionName)
                return false;

            if (other.VaultName != VaultName)
                return false;

            return true;
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

        public string AccountName { get; set; }

        public string AccountKey { get; set; }

        public override bool HasSettings()
        {
            return string.IsNullOrWhiteSpace(StorageContainer) == false;
        }

        public bool Equals(AzureSettings other)
        {
            if (other == null)
                return false;

            if (WasEnabled(other))
                return true;

            return other.RemoteFolderName == RemoteFolderName;
        }
    }
}