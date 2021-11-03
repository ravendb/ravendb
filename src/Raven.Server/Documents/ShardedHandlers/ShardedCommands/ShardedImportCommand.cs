using System.Net.Http;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding;
using Sparrow.Json;

namespace Raven.Server.Documents.ShardedHandlers.ShardedCommands
{
    public class ShardedImportCommand : ShardedCommand
    {
        private MultipartFormDataContent _multipartFormDataContent;
        
        public ShardedImportCommand(ShardedRequestHandler handler, Headers headers, MultipartFormDataContent multi, BlittableJsonReaderObject content = null) : base(handler, headers, content)
        {
            Url = handler.RelativeShardUrl;
            _multipartFormDataContent = multi;
            handler.AddHeaders(this, headers);
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}{Url}";
            var message = new HttpRequestMessage
            {
                Headers =
                {
                    TransferEncodingChunked = true
                },
                Method = Method,
                Content = _multipartFormDataContent,
            };
            foreach ((string key, string value) in Headers)
            {
                if (value == null)
                    continue;

                message.Headers.TryAddWithoutValidation(key, value);
            }
            return message;
        }
    }
}
