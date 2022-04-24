using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetRevisionsBinEntryCommand : RavenCommand<BlittableArrayResult>
    {
        private readonly int _start;
        private readonly int? _pageSize;
        private readonly string _continuationToken;

        public GetRevisionsBinEntryCommand(int start, int? pageSize)
        {
            _start = start;
            _pageSize = pageSize;
        }

        public GetRevisionsBinEntryCommand(string continuationToken)
        {
            _continuationToken = continuationToken;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };

            var pathBuilder = new StringBuilder(node.Url);
            pathBuilder.Append("/databases/")
                .Append(node.Database)
                .Append("/revisions/bin?&start=")
                .Append(_start);

            if (_pageSize.HasValue)
                pathBuilder.Append("&pageSize=").Append(_pageSize);

            if (string.IsNullOrEmpty(_continuationToken) == false)
                pathBuilder.Append("&continuationToken=").Append(_continuationToken);

            url = pathBuilder.ToString();
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                throw new InvalidOperationException();
            if (fromCache)
            {
                // we have to clone the response here because  otherwise the cached item might be freed while
                // we are still looking at this result, so we clone it to the side
                response = response.Clone(context);
            }
            
            Result = JsonDeserializationClient.BlittableArrayResult(response);
        }

        public override bool IsReadRequest => true;
    }
}
