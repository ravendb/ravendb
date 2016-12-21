using System;
using System.IO;
using System.Net.Http;
using Sparrow.Json;
using Raven.NewClient.Client.Http;
using System.Threading.Tasks;

namespace Raven.NewClient.Client.Commands
{
    public class StreamCommand : RavenCommand<StreamResult>
    {
        public string Index;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };

            url = $"{node.Url}/databases/{node.Database}/{Index}";

            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response)
        {
            throw new NotSupportedException();
        }

        public override async Task ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
        {
            Result.Response = response;
            Result.Stream = await response.Content.ReadAsStreamAsync();
        }
    }
}