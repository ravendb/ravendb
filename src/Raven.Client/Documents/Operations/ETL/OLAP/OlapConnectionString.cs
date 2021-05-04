using System.Collections.Generic;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.OLAP
{
    public class OlapConnectionString : ConnectionString
    {
        public override ConnectionStringType Type => ConnectionStringType.Olap;

        public LocalSettings LocalSettings { get; set; }

        public S3Settings S3Settings { get; set; }

        public AzureSettings AzureSettings { get; set; }

        protected override void ValidateImpl(ref List<string> errors)
        {
            if (S3Settings != null)
            {
                if (S3Settings.HasSettings() == false)
                    errors.Add($"{nameof(S3Settings)} has no valid setting. '{nameof(S3Settings.BucketName)}' and '{nameof(GetBackupConfigurationScript)}' are both null");

                return;
            }
            if (AzureSettings != null)
            {
                if (AzureSettings.HasSettings() == false)
                    errors.Add($"{nameof(AzureSettings)} has no valid setting. '{nameof(AzureSettings.StorageContainer)}' and '{nameof(GetBackupConfigurationScript)}' are both null");

                return;
            }
            if (LocalSettings == null)
            {
                errors.Add($"Connection string is missing {nameof(LocalSettings)}, {nameof(S3Settings)} and {nameof(AzureSettings)}");
                return;
            }

            if (LocalSettings.HasSettings() == false)
                errors.Add($"{nameof(LocalSettings)} has no valid setting");
        }

        internal string GetDestination()
        {
            string type, destination;
            if (S3Settings != null)
            {
                type = nameof(RestoreType.S3);
                destination = S3Settings.BucketName;
                if (string.IsNullOrEmpty(S3Settings.RemoteFolderName) == false)
                    destination = $"{destination}/{S3Settings.RemoteFolderName}";
            }
            else if (AzureSettings != null)
            {
                type = nameof(RestoreType.Azure);
                destination = AzureSettings.StorageContainer;
                if (string.IsNullOrEmpty(AzureSettings.RemoteFolderName) == false)
                    destination = $"{destination}/{AzureSettings.RemoteFolderName}";
            }
            else
            {
                type = nameof(RestoreType.Local);
                destination = $"{LocalSettings?.FolderPath ?? "Temp"}";
            }

            return $"{type}-destination@{destination}";
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(LocalSettings)] = LocalSettings?.ToJson();
            json[nameof(S3Settings)] = S3Settings?.ToJson();
            json[nameof(AzureSettings)] = AzureSettings?.ToJson();
            return json;
        }
    }
}
