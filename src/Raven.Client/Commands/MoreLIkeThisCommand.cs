using System;
using System.Net.Http;
using Raven.Client.Data.Queries;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Commands
{
    public class MoreLikeThisCommand : RavenCommand<MoreLikeThisQueryResult>
    {
        private readonly MoreLikeThisQuery _query;

        public MoreLikeThisCommand(MoreLikeThisQuery query)
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            _query = query;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var requestUrl = _query.GetRequestUri();
            EnsureIsNotNullOrEmpty(requestUrl, "url");

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };

            url = $"{node.Url}/databases/{node.Database}" + requestUrl;
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            Result = JsonDeserializationClient.MoreLikeThisQueryResult(response);
        }

        public override bool IsReadRequest => true;
    }
}