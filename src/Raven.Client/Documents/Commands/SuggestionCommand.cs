using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Queries.Suggestion;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class SuggestionCommand : RavenCommand<SuggestionQueryResult>
    {
        private readonly SuggestionQuery _query;

        public SuggestionCommand(SuggestionQuery query)
        {
            _query = query ?? throw new ArgumentNullException(nameof(query));
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var requestUrl = _query.GetRequestUri();
            EnsureIsNotNullOrEmpty(requestUrl, nameof(url));

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

            Result = JsonDeserializationClient.SuggestQueryResult(response);
        }

        public override bool IsReadRequest => true;
    }
}
