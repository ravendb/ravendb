using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json.Parsing;
using static Raven.Client.Documents.Operations.Backups.BackupConfiguration;

namespace Raven.Client.Documents.Operations.ETL.OLAP
{
    public class OlapConnectionString : ConnectionString
    {
        public override ConnectionStringType Type => ConnectionStringType.Olap;

        public LocalSettings LocalSettings { get; set; }

        public S3Settings S3Settings { get; set; }

        public AzureSettings AzureSettings { get; set; }

        public GlacierSettings GlacierSettings { get; set; }

        public GoogleCloudSettings GoogleCloudSettings { get; set; }

        public FtpSettings FtpSettings { get; set; }

        protected override void ValidateImpl(ref List<string> errors)
        {
            if (S3Settings != null)
            {
                if (S3Settings.HasSettings() == false)
                    errors.Add($"{nameof(S3Settings)} has no valid setting. '{nameof(S3Settings.BucketName)}' and '{nameof(GetBackupConfigurationScript)}' are both null");
            }
            if (AzureSettings != null)
            {
                if (AzureSettings.HasSettings() == false)
                    errors.Add($"{nameof(AzureSettings)} has no valid setting. '{nameof(AzureSettings.StorageContainer)}' and '{nameof(GetBackupConfigurationScript)}' are both null");
            }
            if (GlacierSettings != null)
            {
                if (GlacierSettings.HasSettings() == false)
                    errors.Add($"{nameof(GlacierSettings)} has no valid setting. '{nameof(GlacierSettings.VaultName)}' and '{nameof(GetBackupConfigurationScript)}' are both null");
            }
            if (GoogleCloudSettings != null)
            {
                if (GoogleCloudSettings.HasSettings() == false)
                    errors.Add($"{nameof(GoogleCloudSettings)} has no valid setting. '{nameof(GoogleCloudSettings.BucketName)}' and '{nameof(GetBackupConfigurationScript)}' are both null");
            }
            if (FtpSettings != null)
            {
                if (FtpSettings.HasSettings() == false)
                    errors.Add($"{nameof(FtpSettings)} has no valid setting. '{nameof(FtpSettings.Port)}' is 0  and '{nameof(FtpSettings.Url)}' and '{nameof(GetBackupConfigurationScript)}' are both null");
            }
            if (LocalSettings != null)
            {
                if (LocalSettings.HasSettings() == false)
                    errors.Add($"{nameof(LocalSettings)} has no valid setting. '{nameof(LocalSettings.FolderPath)}' and '{nameof(GetBackupConfigurationScript)}' are both null");
            }
        }

        internal string GetDestination()
        {
            const string format = "{0}destination@{1}";
            StringBuilder sb = new StringBuilder();
            string type, destination;
            if (S3Settings != null)
            {
                type = nameof(BackupDestination.AmazonS3);
                destination = S3Settings.BucketName;
                if (string.IsNullOrEmpty(S3Settings.RemoteFolderName) == false)
                    destination = $"{destination}/{S3Settings.RemoteFolderName}";
                sb.AppendFormat(format, type, destination);
            }
            if (AzureSettings != null)
            {
                if (sb.Length > 0)
                    sb.Append(',');

                type = nameof(BackupDestination.Azure);
                destination = AzureSettings.StorageContainer;
                if (string.IsNullOrEmpty(AzureSettings.RemoteFolderName) == false)
                    destination = $"{destination}/{AzureSettings.RemoteFolderName}";
                sb.AppendFormat(format, type, destination);

            }
            if (GlacierSettings != null)
            {
                if (sb.Length > 0)
                    sb.Append(',');

                type = nameof(BackupDestination.AmazonGlacier);
                destination = GlacierSettings.VaultName;
                if (string.IsNullOrEmpty(GlacierSettings.RemoteFolderName) == false)
                    destination = $"{destination}/{GlacierSettings.RemoteFolderName}";
                sb.AppendFormat(format, type, destination);
            }
            if (GoogleCloudSettings != null)
            {
                if (sb.Length > 0)
                    sb.Append(',');

                type = nameof(BackupDestination.GoogleCloud);
                destination = GoogleCloudSettings.BucketName;
                if (string.IsNullOrEmpty(GoogleCloudSettings.RemoteFolderName) == false)
                    destination = $"{destination}/{GoogleCloudSettings.RemoteFolderName}";
                sb.AppendFormat(format, type, destination);
            }
            if (FtpSettings != null)
            {
                if (sb.Length > 0)
                    sb.Append(',');

                type = nameof(BackupDestination.FTP);
                destination = FtpSettings.Url;
                sb.AppendFormat(format, type, destination);
            }
            if (LocalSettings != null)
            {
                type = nameof(BackupDestination.Local);
                destination = $"{LocalSettings?.FolderPath ?? "CoreDirectory"}";
                sb.AppendFormat(format, type, destination);
            }

            return sb.ToString();
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(LocalSettings)] = LocalSettings?.ToJson();
            json[nameof(S3Settings)] = S3Settings?.ToJson();
            json[nameof(AzureSettings)] = AzureSettings?.ToJson();
            json[nameof(GlacierSettings)] = GlacierSettings?.ToJson();
            json[nameof(GoogleCloudSettings)] = GoogleCloudSettings?.ToJson();
            json[nameof(FtpSettings)] = FtpSettings?.ToJson();
            return json;
        }
    }
}
