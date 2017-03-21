using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Server.Commands
{
    public class GetTopologyCommand : RavenCommand<Topology>
    {
        private string _forcedUrl;

        public GetTopologyCommand(string forcedUrl = null)
        {
            AvoidFailover = true;
            _forcedUrl = forcedUrl;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/topology?&name={node.Database}";
            if (string.IsNullOrEmpty(_forcedUrl) == false)
            {
                url += $"&url={_forcedUrl}";
            }
            return new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                return;

            Result = JsonDeserializationClient.ClusterTopology(response);
        }

        public override bool IsReadRequest => true;
    }
}