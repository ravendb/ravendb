using System;

namespace Raven.Client.Documents.Operations.ETL
{
    // todo 

    public class S3EtlConfiguration : EtlConfiguration<S3ConnectionString>
    {
        public string TempDirectoryPath { get; set; }

        public TimeSpan ETLFrequency { get; set; }

        public S3EtlConfiguration()
        {
        }

        public override string GetDestination()
        {
            throw new NotImplementedException();
        }

        public override EtlType EtlType => EtlType.S3;

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
