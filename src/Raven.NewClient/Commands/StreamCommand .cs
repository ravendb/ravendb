using System;
using System.IO;
using System.Net.Http;
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
            throw new NotImplementedException();
        }

        public void SetResponse(Stream response)
        {
            //WIP
            if (response == null)
                throw new InvalidOperationException();
            var buffer = new byte[12];
            response.Read(buffer, 0, 12);
            //TODO  - better exception
            if (string.Compare(buffer.ToString(), "{\"Results\":[") != 1)
                throw new InvalidOperationException();
            Result = new StreamResult {Results = response};
        }
    }
}