using System;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide.Operations;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Commands
{
    public class NodeInfo
    {
        public string NodeTag;
        public string TopologyId;
        public string Certificate;
        public string ClusterStatus;
        public int NumberOfCores;
        public double InstalledMemoryInGb;
        public double UsableMemoryInGb;
        public BuildNumber BuildInfo;
        public Guid ServerId;
        public RachisState CurrentState;
    }

    public class GetNodeInfoCommand : RavenCommand<NodeInfo>
    {
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/cluster/node-info";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                return;

            Result = JsonDeserializationClient.NodeInfo(response);
        }

        public override bool IsReadRequest => true;
    }
}
