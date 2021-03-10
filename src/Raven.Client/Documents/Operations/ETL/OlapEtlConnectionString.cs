using System.Collections.Generic;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL
{
    public class OlapEtlConnectionString : ConnectionString
    {
        public override ConnectionStringType Type => ConnectionStringType.Olap;

        public OlapEtlLocalSettings LocalSettings { get; set; }

        public S3Settings S3Settings { get; set; }

        public override void ValidateImpl(ref List<string> errors)
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

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(LocalSettings)] = LocalSettings?.ToJson();
            json[nameof(S3Settings)] = S3Settings?.ToJson();
            return json;
        }
    }

    public class OlapEtlLocalSettings : LocalSettings
    {
        public bool KeepFilesOnDisc { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(KeepFilesOnDisc)] = KeepFilesOnDisc;
            return json;
        }
    }
}
