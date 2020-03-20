using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public class ServerWideExternalReplication : ExternalReplication
    {
        internal static string NamePrefix = "Server Wide External Replication";

        internal static string RavenConnectionStringPrefix = "Server Wide Raven Connection String";

        public string[] TopologyDiscoveryUrls;

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(NamePrefix)] = NamePrefix;
            json[nameof(TopologyDiscoveryUrls)] = TopologyDiscoveryUrls;
            return json;
        }
    }
}
