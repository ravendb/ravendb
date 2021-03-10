using System;

namespace Raven.Client.Documents.Operations.ETL
{
    // todo 

    public class OlapEtlConfiguration : EtlConfiguration<OlapEtlConnectionString>
    {
        public TimeSpan ETLFrequency { get; set; }

        public OlapEtlConfiguration()
        {
        }

        public override string GetDestination() => ConnectionStringName;

        public override EtlType EtlType => EtlType.Olap;

        public override bool UsingEncryptedCommunicationChannel()
        {
            throw new NotImplementedException();
        }

        public override string GetDefaultTaskName()
        {
            return $"OLAP ETL to {ConnectionStringName}";
        }
    }
}
