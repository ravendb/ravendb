using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Streaming
{
    public class PostQueryStreamCommand : RavenCommand<StreamResult>
    {
        private readonly BlittableJsonReaderObject _indexQueryServerSide;
        private readonly string _debug;
        private readonly bool _ignoreLimit;

        public PostQueryStreamCommand(BlittableJsonReaderObject indexQueryServerSide, string debug, bool ignoreLimit)
        {
            _indexQueryServerSide = indexQueryServerSide;
            _debug = debug;
            _ignoreLimit = ignoreLimit;
            ResponseType = RavenCommandResponseType.Empty;
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async (stream) => await _indexQueryServerSide.WriteJsonToAsync(stream))
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
            var responseStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);

            Result = new StreamResult
            {
                Response = response,
                Stream = new StreamWithTimeout(responseStream) //TODO stav: leave as stream with timeout?
            };

            return ResponseDisposeHandling.Manually;
        }
    }
}
