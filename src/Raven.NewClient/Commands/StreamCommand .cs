using System;
using System.Net.Http;
using Sparrow.Json;
using Raven.NewClient.Client.Http;
using System.Threading.Tasks;

namespace Raven.NewClient.Client.Commands
{
    public class StreamCommand : RavenCommand<StreamResult>
    {
        public readonly bool UsedTransformer;

        private readonly string _url;

        public StreamCommand(string url, bool usedTransformer)
        {
            if (url == null)
                throw new ArgumentNullException(nameof(url));

            _url = url;
            UsedTransformer = usedTransformer;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };

            url = $"{node.Url}/databases/{node.Database}/{_url}";

            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            throw new NotSupportedException();
        }

        public override async Task ProcessResponse(JsonOperationContext context, HttpCache cache, RequestExecuterOptions options, HttpResponseMessage response, string url)
        {
            Result = new StreamResult
            {
                Response = response,
                Stream = await response.Content.ReadAsStreamAsync()
            };
        }

        public override bool IsReadRequest => true;
    }
}