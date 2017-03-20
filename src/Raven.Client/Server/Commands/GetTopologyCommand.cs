using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Server.Commands
{
    public class GetTopologyCommand : RavenCommand<Topology>
    {
        public GetTopologyCommand()
        {
            AvoidFailover = true;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/databases/topology?&name={node.Database}";
            return new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.ClusterTopology(response);
        }

        public override bool IsReadRequest => true;
    }
}