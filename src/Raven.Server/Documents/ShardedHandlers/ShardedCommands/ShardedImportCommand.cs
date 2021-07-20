using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding;
using Sparrow.Json;

namespace Raven.Server.Documents.ShardedHandlers.ShardedCommands
{
    public class ShardedImportCommand : ShardedCommand
    {
        public readonly BlittableJsonReaderObject Content;
        public readonly Dictionary<string, string> Headers = new Dictionary<string, string>();
        public string Url;
        public readonly HttpMethod Method;
        private MultipartFormDataContent mm;
        public HttpResponseMessage Response;
        public string guid;

        public ShardedImportCommand(ShardedRequestHandler handler, Headers headers, MultipartFormDataContent multi, BlittableJsonReaderObject content =null) : base(handler, headers, content)
        {
            Method = handler.Method;
            Url = handler.RelativeShardUrl;
            Content = content;
            mm = multi;
            handler.AddHeaders(this, headers);
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}{Url}&Guid={guid}";
            var message = new HttpRequestMessage
            {
                Headers =
                {
                    TransferEncodingChunked = true
                },
                Method = Method,
                Content = mm,
            };
            foreach ((string key, string value) in Headers)
            {
                if (value == null) //TODO sharding: make sure it is okay to skip null
                    continue;

                message.Headers.TryAddWithoutValidation(key, value);
            }
            return message;
        }
    }
}
