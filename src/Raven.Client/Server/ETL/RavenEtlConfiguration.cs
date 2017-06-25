using System;

namespace Raven.Client.Server.ETL
{
    public class RavenEtlConfiguration : EtlConfiguration<RavenConnectionString>
    {
        private string _destination;

        public int? LoadRequestTimeoutInSec { get; set; }

        public override EtlType EtlType => EtlType.Raven;

        public override string GetDestination()
        {
            return _destination ?? (_destination = $"{Connection.Database}@{Connection.Url}");
        }

        public override bool UsingEncryptedCommunicationChannel()
        {
            return Connection.Url?.StartsWith("https:", StringComparison.OrdinalIgnoreCase) == true;
        }
    }
}