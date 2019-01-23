using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Commands
{
    public class GetClusterTopologyCommand : RavenCommand<ClusterTopologyResponse>
    {
        private readonly string _debugTag;

        public GetClusterTopologyCommand()
        {
        }

        public GetClusterTopologyCommand(string debugTag)
        {
            _debugTag = debugTag;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/cluster/topology";
            if (_debugTag != null)
                url += "?" + _debugTag;

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                return;

            Result = JsonDeserializationClient.ClusterTopology(response);
        }

        public override bool IsReadRequest => true;
    }
}
