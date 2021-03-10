using System;

namespace Raven.Client.Documents.Operations.ETL
{
    // todo 

    public class ParquetEtlConfiguration : EtlConfiguration<ParquetEtlConnectionString>
    {
        public TimeSpan ETLFrequency { get; set; }

        public ParquetEtlConfiguration()
        {
        }

        public override string GetDestination() => ConnectionStringName;

        public override EtlType EtlType => EtlType.Parquet;

        public override bool UsingEncryptedCommunicationChannel()
        {
            throw new NotImplementedException();
        }

        public override string GetDefaultTaskName()
        {
            return $"Parquet ETL to {ConnectionStringName}";
        }
    }
}
