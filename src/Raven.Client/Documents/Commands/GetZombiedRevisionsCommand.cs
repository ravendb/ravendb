using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetZombiedRevisionsCommand : RavenCommand<BlittableArrayResult>
    {
        private readonly int? _start;
        private readonly int? _pageSize;

        public GetZombiedRevisionsCommand(int? start, int? pageSize)
        {
            _start = start;
            _pageSize = pageSize;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };

            var pathBuilder = new StringBuilder(node.Url);
            pathBuilder.Append("/databases/")
                .Append(node.Database)
                .Append("/revisions/zombied?");

            if (_start.HasValue)
                pathBuilder.Append("&start=").Append(_start);
            if (_pageSize.HasValue)
                pathBuilder.Append("&pageSize=").Append(_pageSize);

            url = pathBuilder.ToString();
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