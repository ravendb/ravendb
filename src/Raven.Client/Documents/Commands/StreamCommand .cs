using System;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
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
            ResponseType = RavenCommandResponseType.Raw;
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

        public override async Task ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
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