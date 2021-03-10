using System;

namespace Raven.Client.Documents.Operations.ETL
{
    // todo 

    public class ParquetEtlConfiguration : EtlConfiguration<ParquetEtlConnectionString>
    {
        public string TempDirectoryPath { get; set; }

        public TimeSpan ETLFrequency { get; set; }

        public ParquetEtlConfiguration()
        {
        }

        public override string GetDestination()
        {
            throw new NotImplementedException();
        }

        public override EtlType EtlType => EtlType.Parquet;

        public override bool UsingEncryptedCommunicationChannel()
        {
            throw new NotImplementedException();
        }

        public override string GetDefaultTaskName()
        {
            throw new NotImplementedException();
        }
    }
}
