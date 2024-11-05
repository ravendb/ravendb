using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Streaming
{
    public sealed class PostQueryStreamCommand : LongRunningRavenCommand<StreamResult>
    {
        private readonly DocumentConventions _conventions;
        private readonly BlittableJsonReaderObject _query;
        private readonly string _debug;
        private readonly bool _ignoreLimit;

        public PostQueryStreamCommand(DocumentConventions conventions, BlittableJsonReaderObject query, string debug, bool ignoreLimit)
        {
            _conventions = conventions;
            _query = query;
            _debug = debug;
            _ignoreLimit = ignoreLimit;
            ResponseType = RavenCommandResponseType.Empty;
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var queryToWrite = _query.CloneForConcurrentRead(ctx);

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post, 
                Content = new BlittableJsonContent(async (stream, token) => await ctx.WriteAsync(stream, queryToWrite, token), _conventions)
            };

            var sb = new StringBuilder($"{node.Url}/databases/{node.Database}/streams/queries?");

            if (string.IsNullOrEmpty(_debug) == false)
            {
                sb.Append("debug=").Append(Uri.EscapeDataString(_debug)).Append("&");
            }
            if (_ignoreLimit)
            {
                sb.Append("ignoreLimit=true");
            }

            url = sb.ToString();
            return request;
        }

        public override async Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
        {
            var responseStream = await response.Content.ReadAsStreamWithZstdSupportAsync().ConfigureAwait(false);
            Result = new StreamResult(responseStream, response);
            return ResponseDisposeHandling.Manually;
        }
    }
}
