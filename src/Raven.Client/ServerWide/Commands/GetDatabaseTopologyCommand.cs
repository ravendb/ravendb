using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Commands
{
    public sealed class GetDatabaseTopologyCommand : RavenCommand<Topology>
    {
        private readonly Guid? _applicationIdentifier;
        private readonly bool _usePrivateUrls;
        private readonly string _debugTag;
        private readonly bool _includePromotables;

        public GetDatabaseTopologyCommand()
        {
            CanCacheAggressively = false;
            Timeout = TimeSpan.FromSeconds(15);
        }

        internal GetDatabaseTopologyCommand(string debugTag, Guid? applicationIdentifier, bool usePrivateUrls, bool includePromotables)
            : this(debugTag, applicationIdentifier)
        {
            _usePrivateUrls = usePrivateUrls;
            _includePromotables = includePromotables;
        }

        public GetDatabaseTopologyCommand(string debugTag, Guid? applicationIdentifier)
            : this(debugTag)
        {
            _applicationIdentifier = applicationIdentifier;
        }

        public GetDatabaseTopologyCommand(string debugTag) : this()
        {
            _debugTag = debugTag;
            Timeout = TimeSpan.FromSeconds(15);
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/topology?name={node.Database}";
            if (_debugTag != null)
                url += "&" + _debugTag;
            if (_applicationIdentifier != null)
                url += "&applicationIdentifier=" + _applicationIdentifier;
            if (_usePrivateUrls)
                url += "&private=true";
            if (_includePromotables)
                url += "&includePromotables=true";

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

        public override async Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url, CancellationToken token)
        {
            var result = await TopologyCommandHelper.ParseTopologyResponseAsync(context, response, url, "database/topology").ConfigureAwait(false);

            if (cache != null && CanCache)
                CacheResponse(cache, url, response, result);

            SetResponse(context, result, fromCache: false);

            if (Result?.Nodes == null)
                TopologyCommandHelper.ThrowUnexpectedTopologyResponse(url, result);

            return ResponseDisposeHandling.Automatic;
        }

        public override bool IsReadRequest => true;
    }
}
