using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetTopologyCommand : RavenCommand<Topology>
    {
        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"topology?url=" + node.Url;
            return new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };
        }

        public override void SetResponse(BlittableJsonReaderObject response)
        {
            Result = JsonDeserializationClient.ClusterTopology(response);
        }
    }
}