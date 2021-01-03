using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class ShardedCommand : RavenCommand<BlittableJsonReaderObject>
    {
        public BlittableJsonReaderObject Content;
        public Dictionary<string, string> Headers = new Dictionary<string, string>();
        public string Url;
        public HttpMethod Method;

        public HttpResponseMessage Response;
        public IDisposable Disposable;
        public List<int> PositionMatches;

        public override bool IsReadRequest => false;
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}{Url}";
            var message = new HttpRequestMessage
            {
                Method = Method,
                Content = Content == null ? null : new BlittableJsonContent(Content),
            };
            foreach ((string key, string value) in Headers)
            {
                message.Headers.Add(key, value);
            }
            return message;
        }


        public override Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
        {
            Response = response;
            return base.ProcessResponse(context, cache, response, url);
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = response;
        }
        internal class BlittableJsonContent : HttpContent
        {
            private readonly BlittableJsonReaderObject _data;

            public BlittableJsonContent(BlittableJsonReaderObject data)
            {
                _data = data;
                
            }

            protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
            {
                _data.WriteJsonTo(stream);
                return Task.CompletedTask;
            }

            protected override bool TryComputeLength(out long length)
            {
                length = -1;
                return false;
            }
        }
    }
}
