using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Sharding.Handlers;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Commands
{
    public abstract class ShardedBaseCommand<T> : RavenCommand<T>
    {
        protected readonly ShardedDatabaseRequestHandler Handler;
        public BlittableJsonReaderObject Content;
        public readonly Dictionary<string, string> Headers = new Dictionary<string, string>();
        public string Url;
        public readonly HttpMethod Method;

        public HttpResponseMessage Response;

        public override bool IsReadRequest => false;

        protected ShardedBaseCommand(ShardedDatabaseRequestHandler handler, HttpMethod method, Headers headers, BlittableJsonReaderObject content = null)
        {
            Handler = handler;
            Method = method;
            Url = handler.RelativeShardUrl;
            Content = content;

            AddDefaultHeaders(this, headers);
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}{Url}";
            var message = new HttpRequestMessage
            {
                Method = Method,
                Content = Content == null ? null : new BlittableJsonContent(async (stream) => await Content.WriteJsonToAsync(stream)),
            };
            foreach ((string key, string value) in Headers)
            {
                if (value == null) //TODO sharding: make sure it is okay to skip null
                    continue;

                message.Headers.TryAddWithoutValidation(key, value);
            }

            return message;
        }

        public override Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
        {
            Response = response;
            return base.ProcessResponse(context, cache, response, url);
        }

        public void AddDefaultHeaders(ShardedBaseCommand<T> command, Headers headers)
        {
            if (headers.HasFlag(Commands.Headers.IfMatch))
            {
                command.Headers[Constants.Headers.IfMatch] = Handler.GetStringFromHeaders(Constants.Headers.IfMatch);
            }
            else if(headers.HasFlag(Commands.Headers.IfNoneMatch))
            {
                command.Headers[Constants.Headers.IfNoneMatch] = Handler.GetStringFromHeaders(Constants.Headers.IfNoneMatch);
            }

            if (headers.HasFlag(Commands.Headers.Sharded))
                command.Headers[Constants.Headers.Sharded] = "true";
            
            var lastKnownClusterTransactionIndex = Handler.GetStringFromHeaders(Constants.Headers.LastKnownClusterTransactionIndex);
            if (lastKnownClusterTransactionIndex != null)
                command.Headers[Constants.Headers.LastKnownClusterTransactionIndex] = lastKnownClusterTransactionIndex;
        }
    }

    public enum Headers
    {
        None = 0,
        IfMatch = 1,
        IfNoneMatch = 2,
        Sharded = 4
    }
}
