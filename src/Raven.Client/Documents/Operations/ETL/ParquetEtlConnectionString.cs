using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL
{
    public enum ParquetEtlDestination
    {
        S3
    }

    public class ParquetEtlConnectionString : ConnectionString
    {
        public ParquetEtlConnectionString(ParquetEtlDestination destinationType)
        {
            Destination = destinationType;
        }

        public ParquetEtlConnectionString()
        {
            // for desiralization 
        }

        public override ConnectionStringType Type => ConnectionStringType.Parquet;

        public ParquetEtlDestination Destination { get; set; }

        public S3Settings S3Settings { get; set; }

        public override void ValidateImpl(ref List<string> errors)
        {
            switch (Destination)
            {
                case ParquetEtlDestination.S3:
                    if (S3Settings == null)
                    {
                        errors.Add($"{nameof(S3Settings)} cannot be null");
                        return;
                    }
                    if (S3Settings.HasSettings() == false)
                        errors.Add($"{nameof(S3Settings)} has no setting");
                    return;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Destination)] = Destination;
            json[nameof(S3Settings)] = S3Settings?.ToJson();
            return json;
        }
    }
}
