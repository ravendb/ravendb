using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Backups
{
    public abstract class BackupSettings
    {
        public bool Disabled { get; set; }

        public GetBackupConfigurationScript GetBackupConfigurationScript { get; set; }

        public virtual bool HasSettings()
        {
            return GetBackupConfigurationScript != null &&
                   string.IsNullOrWhiteSpace(GetBackupConfigurationScript.Exec) == false;
        }

        public virtual bool WasEnabled(BackupSettings other)
        {
            return Disabled && other.Disabled == false;
        }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Disabled)] = Disabled,
                [nameof(GetBackupConfigurationScript)] = GetBackupConfigurationScript?.ToJson()
            };
        }
    }

    public class GetBackupConfigurationScript
    {
        public GetBackupConfigurationScript()
        {
            TimeoutInMs = 10_000;
        }

        internal GetBackupConfigurationScript(GetBackupConfigurationScript script)
        {
            if (script == null)
                throw new ArgumentNullException(nameof(script));

            Arguments = script.Arguments;
            Exec = script.Exec;
            TimeoutInMs = script.TimeoutInMs;
        }

        public string Exec { get; set; }

        public string Arguments { get; set; }

        public int TimeoutInMs { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Exec)] = Exec,
                [nameof(Arguments)] = Arguments,
                [nameof(TimeoutInMs)] = TimeoutInMs
            };
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
            if (base.HasSettings())
                return true;

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

        public override DynamicJsonValue ToJson()
        {
            var djv = base.ToJson();

            djv[nameof(FolderPath)] = FolderPath;

            return djv;
        }
    }

    public abstract class AmazonSettings : BackupSettings, ICloudBackupSettings
    {
        public string AwsAccessKey { get; set; }

        public string AwsSecretKey { get; set; }

        public string AwsSessionToken { get; set; }

        /// <summary>
        /// Amazon Web Services (AWS) region.
        /// </summary>
        public string AwsRegionName { get; set; }

        /// <summary>
        /// Remote folder name.
        /// </summary>
        public string RemoteFolderName { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var djv = base.ToJson();

            djv[nameof(AwsAccessKey)] = AwsAccessKey;
            djv[nameof(AwsSecretKey)] = AwsSecretKey;
            djv[nameof(AwsRegionName)] = AwsRegionName;
            djv[nameof(AwsSessionToken)] = AwsSessionToken;
            djv[nameof(RemoteFolderName)] = RemoteFolderName;

            return djv;
        }
    }

    public class S3Settings : AmazonSettings
    {
        /// <summary>
        /// S3 Bucket name.
        /// </summary>
        public string BucketName { get; set; }

        /// <summary>
        /// S3 server Url when using custom server
        /// </summary>
        public string CustomServerUrl { get; set; }

        public override bool HasSettings()
        {
            if (base.HasSettings())
                return true;

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

            if (other.CustomServerUrl != CustomServerUrl)
                return false;

            return true;
        }

        public override DynamicJsonValue ToJson()
        {
            var djv = base.ToJson();
            djv[nameof(BucketName)] = BucketName;
            djv[nameof(CustomServerUrl)] = CustomServerUrl;
            return djv;
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
            if (base.HasSettings())
                return true;

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

            if (other.RemoteFolderName != RemoteFolderName)
                return false;

            return true;
        }

        public override DynamicJsonValue ToJson()
        {
            var djv = base.ToJson();
            djv[nameof(VaultName)] = VaultName;
            return djv;
        }
    }

    public class AzureSettings : BackupSettings, ICloudBackupSettings
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

        public string SasToken { get; set; }

        public AzureSettings()
        {
        }

        internal AzureSettings(AzureSettings settings)
        {
            if (settings == null)
                throw new ArgumentNullException(nameof(settings));

            AccountKey = settings.AccountKey;
            AccountName = settings.AccountName;
            RemoteFolderName = settings.RemoteFolderName;
            SasToken = settings.SasToken;
            StorageContainer = settings.StorageContainer;
            Disabled = settings.Disabled;

            if (settings.GetBackupConfigurationScript != null)
                GetBackupConfigurationScript = new GetBackupConfigurationScript(settings.GetBackupConfigurationScript);
        }

        public override bool HasSettings()
        {
            if (base.HasSettings())
                return true;

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

        public override DynamicJsonValue ToJson()
        {
            var djv = base.ToJson();

            djv[nameof(StorageContainer)] = StorageContainer;
            djv[nameof(RemoteFolderName)] = RemoteFolderName;
            djv[nameof(AccountName)] = AccountName;
            djv[nameof(AccountKey)] = AccountKey;
            djv[nameof(SasToken)] = SasToken;

            return djv;
        }
    }

    public class FtpSettings : BackupSettings
    {
        public string Url { get; set; }

        public int? Port { get; set; }

        public string UserName { get; set; }

        public string Password { get; set; }

        public string CertificateAsBase64 { get; set; }

        public string CertificateFileName { get; set; }

        public override bool HasSettings()
        {
            if (base.HasSettings())
                return true;

            return Port != 0 && string.IsNullOrWhiteSpace(Url) == false;
        }

        public override DynamicJsonValue ToJson()
        {
            var djv = base.ToJson();

            djv[nameof(Url)] = Url;
            djv[nameof(Port)] = Port;
            djv[nameof(UserName)] = UserName;
            djv[nameof(Password)] = Password;
            djv[nameof(CertificateAsBase64)] = CertificateAsBase64;
            djv[nameof(CertificateFileName)] = CertificateFileName;

            return djv;
        }
    }
    public class GoogleCloudSettings : BackupSettings, ICloudBackupSettings
    {
        /// <summary>
        /// Google cloud storage bucket name must be globally unique
        /// </summary>
        public string BucketName { get; set; }

        /// <summary>
        /// Path to remote bucket folder.
        /// </summary>
        public string RemoteFolderName { get; set; }

        /// <summary>
        /// Authentication credentials to your Google Cloud Storage.
        /// </summary>
        public string GoogleCredentialsJson { get; set; }

        public override bool HasSettings()
        {
            return string.IsNullOrWhiteSpace(BucketName) == false;
        }

        public bool Equals(GoogleCloudSettings other)
        {
            if (other == null)
                return false;

            if (WasEnabled(other))
                return true;

            return other.RemoteFolderName == RemoteFolderName;
        }

        public override DynamicJsonValue ToJson()
        {
            var djv = base.ToJson();

            djv[nameof(BucketName)] = BucketName;
            djv[nameof(RemoteFolderName)] = RemoteFolderName;
            djv[nameof(GoogleCredentialsJson)] = GoogleCredentialsJson;

            return djv;
        }
    }

    public interface ICloudBackupSettings
    {
        public string RemoteFolderName { get; set; }
    }
}
