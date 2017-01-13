using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Extensions;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    public class MoreLikeThisCommand : RavenCommand<GetDocumentResult>
    {
        public MoreLikeThisQuery Query;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var requestUrl = Query.GetRequestUri();
            EnsureIsNotNullOrEmpty(requestUrl, "url");

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };

            url = $"{node.Url}/databases/{node.Database}" + requestUrl;
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            Result = JsonDeserializationClient.GetDocumentResult(response);
        }

        public override bool IsReadRequest => true;
    }
}