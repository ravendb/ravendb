using System;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Commands
{
    public class GetRevisionCommand : RavenCommand<BlittableArrayResult>
    {
        private readonly string _id;
        private readonly int _start;
        private readonly int _pageSize;

        public GetRevisionCommand(string id, int start, int pageSize)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            _id = id;
            _start = start;
            _pageSize = pageSize;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };

            url = $"{node.Url}/databases/{node.Database}/revisions?id={Uri.EscapeUriString(_id)}&start={_start}&pageSize={_pageSize}";
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