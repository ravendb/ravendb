using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Sharding;
using Sparrow.Json;

namespace Raven.Server.Documents.ShardedHandlers.ShardedCommands
{
    public abstract class ShardedBaseCommand<T> : RavenCommand<T>
    {
        public readonly BlittableJsonReaderObject Content;
        public readonly Dictionary<string, string> Headers = new Dictionary<string, string>();
        public string Url;
        public readonly HttpMethod Method;

        public HttpResponseMessage Response;
        

        public override bool IsReadRequest => false;

        public ShardedBaseCommand(ShardedRequestHandler handler, Headers headers, BlittableJsonReaderObject content = null)
        {
            Method = handler.Method;
            Url = handler.RelativeShardUrl;
            Content = content;

            handler.AddHeaders(this, headers);
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}{Url}";
            var message = new HttpRequestMessage
            {
                Method = Method, 
                Content = Content == null ? null : new BlittableJsonContent((stream)=> Content.WriteJsonTo(stream)),
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
    }
    
    public enum Headers
    {
        None,
        IfMatch,
        IfNoneMatch,
    }
}
