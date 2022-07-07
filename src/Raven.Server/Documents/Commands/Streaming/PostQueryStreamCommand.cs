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
        private readonly string _format;
        private readonly string _debug;
        private readonly bool _ignoreLimit;
        private readonly string _properties;

        public PostQueryStreamCommand(BlittableJsonReaderObject indexQueryServerSide, string format, string debug, bool ignoreLimit, string properties)
        {
            _indexQueryServerSide = indexQueryServerSide;
            _format = format;
            _debug = debug;
            _ignoreLimit = ignoreLimit;
            _properties = properties;
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

            var sb = new StringBuilder($"{node.Url}/databases/{node.Database}/streams/queries?"); //TODO stav: trailing '?' allowed?

            if (string.IsNullOrEmpty(_format) == false)
            {
                sb.Append("format=").Append(Uri.EscapeDataString(_format)).Append("&");
            }
            if (string.IsNullOrEmpty(_debug) == false)
            {
                sb.Append("debug=").Append(Uri.EscapeDataString(_debug)).Append("&");
            }
            if (_ignoreLimit)
            {
                sb.Append("ignoreLimit=true");
            }
            if (string.IsNullOrEmpty(_properties) == false)
            {
                sb.Append("field=").Append(Uri.EscapeDataString(_properties)).Append("&");
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

            return ResponseDisposeHandling.Manually; //TODO stav: ?
        }
    }
}
