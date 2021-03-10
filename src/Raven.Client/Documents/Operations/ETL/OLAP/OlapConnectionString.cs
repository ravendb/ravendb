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

        protected override void ValidateImpl(ref List<string> errors)
        {
            if (S3Settings != null)
            {
                if (S3Settings.HasSettings() == false)
                    errors.Add($"{nameof(S3Settings)} has no valid setting");

                return;
            }

            if (LocalSettings == null)
            {
                errors.Add($"Missing both {nameof(LocalSettings)} and {nameof(S3Settings)}");
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
            else
            {
                type = nameof(RestoreType.Local);
                destination = $"{LocalSettings.FolderPath ?? "Temp"}";
            }

            return $"{type}-destination@{destination}";
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(LocalSettings)] = LocalSettings?.ToJson();
            json[nameof(S3Settings)] = S3Settings?.ToJson();
            return json;
        }
    }
}
