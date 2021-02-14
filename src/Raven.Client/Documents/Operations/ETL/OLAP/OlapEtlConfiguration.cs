using System;

namespace Raven.Client.Documents.Operations.ETL.OLAP
{
    public class OlapEtlConfiguration : EtlConfiguration<OlapConnectionString>
    {
        public TimeSpan RunFrequency { get; set; }

        public OlapEtlFileFormat Format { get; set; }

        public bool KeepFilesOnDisk { get; set; }

        public string CustomPrefix { get; set; }

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
