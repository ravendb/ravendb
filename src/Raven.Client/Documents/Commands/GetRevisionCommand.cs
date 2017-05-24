using System;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetRevisionCommand : RavenCommand<BlittableArrayResult>
    {
        private readonly string _id;
        private readonly int _start;
        private readonly int _pageSize;

        public GetRevisionCommand(string id, int start, int pageSize)
        {
            _id = id ?? throw new ArgumentNullException(nameof(id));
            _start = start;
            _pageSize = pageSize;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };

            url = $"{node.Url}/databases/{node.Database}/revisions?id={Uri.EscapeDataString(_id)}&start={_start}&pageSize={_pageSize}";
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                throw new InvalidOperationException();
            Result = JsonDeserializationClient.BlittableArrayResult(response);
        }

        public override bool IsReadRequest => true;
    }
}