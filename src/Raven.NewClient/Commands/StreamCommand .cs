using System;
using System.Collections;
using System.Linq;
using System.Net.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;
using Raven.NewClient.Client.Http;

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
            if (response == null)
                throw new InvalidOperationException();
            Result = JsonDeserializationClient.StreamResult(response);
        }
    }
}