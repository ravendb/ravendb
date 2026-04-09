using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Commands
{
    public sealed class GetClusterTopologyCommand : RavenCommand<ClusterTopologyResponse>
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

        public override async Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
        {
            var result = await TopologyCommandHelper.ParseTopologyResponseAsync(context, response, url, "cluster/topology").ConfigureAwait(false);

            if (cache != null && CanCache)
                CacheResponse(cache, url, response, result);

            SetResponse(context, result, fromCache: false);

            if (Result?.Topology == null)
                TopologyCommandHelper.ThrowUnexpectedTopologyResponse(url, result);

            return ResponseDisposeHandling.Automatic;
        }

        public override bool IsReadRequest => true;
    }
}
