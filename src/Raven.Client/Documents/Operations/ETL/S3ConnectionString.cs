using System.Collections.Generic;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL
{
    public class S3ConnectionString : ConnectionString
    {
        public override ConnectionStringType Type => ConnectionStringType.S3;

        public S3Settings S3Settings { get; set; } 

        protected override void ValidateImpl(ref List<string> errors)
        {
            if (S3Settings == null)
                errors.Add($"{nameof(S3Settings)} cannot be null");

            // todo
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(S3Settings)] = S3Settings.ToJson();
            return json;
        }
    }
}
