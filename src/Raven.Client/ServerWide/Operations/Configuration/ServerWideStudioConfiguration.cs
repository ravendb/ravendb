using Raven.Client.Documents.Operations.Configuration;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public class ServerWideStudioConfiguration : StudioConfiguration
    {
        public int? ReplicationFactor { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(ReplicationFactor)] = ReplicationFactor;
            return json;
        }
    }
}
