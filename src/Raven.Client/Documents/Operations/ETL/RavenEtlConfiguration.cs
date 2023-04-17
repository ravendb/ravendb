using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL
{
    public class RavenEtlConfiguration : EtlConfiguration<RavenConnectionString>
    {
        private string _destination;

        public int? LoadRequestTimeoutInSec { get; set; }

        public override EtlType EtlType => EtlType.Raven;

        public override string GetDestination()
        {
            return _destination ?? (_destination = $"{Connection.Database}@{string.Join(",",Connection.TopologyDiscoveryUrls)}");
        }

        public override bool UsingEncryptedCommunicationChannel()
        {
            foreach (var url in Connection.TopologyDiscoveryUrls)
            {
                if (url.StartsWith("http:", StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
            }
            return true;
        }

        public override string GetDefaultTaskName()
        {
            return $"RavenDB ETL to {ConnectionStringName}";
        }

        public override DynamicJsonValue ToJson()
        {
            var result = base.ToJson();

            result[nameof(LoadRequestTimeoutInSec)] = LoadRequestTimeoutInSec;

            return result;
        }
    }
}
