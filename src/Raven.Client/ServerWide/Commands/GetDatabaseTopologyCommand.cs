using System;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Commands
{
    public class GetDatabaseTopologyCommand : RavenCommand<Topology>
    {
        private readonly string _debugTag;

        public GetDatabaseTopologyCommand()
        {
            CanCacheAggressively = false;
        }

        public GetDatabaseTopologyCommand(string debugTag)
        {
            _debugTag = debugTag;
            CanCacheAggressively = false;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/topology?name={node.Database}";
            if (_debugTag != null)
                url += "&" + _debugTag;

            if (node.Url.IndexOf(".fiddler", StringComparison.OrdinalIgnoreCase) != -1)
            {
                // we want to keep the '.fiddler' stuff there so we'll keep tracking request
                // so we are going to ask the server to respect it
                url += "&localUrl=" + Uri.EscapeDataString(node.Url);
            }

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                return;

            Result = JsonDeserializationClient.Topology(response);
        }

        public override bool IsReadRequest => true;
    }
}
