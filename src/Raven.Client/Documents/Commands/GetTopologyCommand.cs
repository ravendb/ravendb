using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetTopologyCommand : RavenCommand<Topology>
    {
        public override HttpRequestMessage CreateRequest(out string url)
        {
            url = $"topology";
            return new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };
        }

        public override void SetResponse(BlittableJsonReaderObject response)
        {
            Result = JsonDeserialization.ClusterTopology(response);
        }
    }
}