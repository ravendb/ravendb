using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    public class GetFacetsCommand : RavenCommand<FacetedQueryResult>
    {
        public FacetQuery Query;
        public JsonOperationContext Context;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            if (string.IsNullOrWhiteSpace(Query.FacetSetupDoc) == false && Query.Facets != null && Query.Facets.Count > 0)
                throw new InvalidOperationException($"You cannot specify both '{nameof(FacetQuery.FacetSetupDoc)}' and '{nameof(FacetQuery.Facets)}'.");

            //TODO - EFRAT
            var method = Query.CalculateHttpMethod();

            var request = new HttpRequestMessage
            {
                Method = method
            };
            
            if (method == HttpMethod.Post)
            {
                request.Content = new BlittableJsonContent(stream =>
                {
                    using (var writer = new BlittableJsonTextWriter(Context, stream))
                    {
                        Context.Write(writer, Query.GetFacetsAsJson());
                    }
                });
            }
            url = $"{node.Url}/databases/{node.Database}/queries/{Query.IndexName}?{Query.GetQueryString(method)}";
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            Result = JsonDeserializationClient.FacetedQueryResult(response);
        }

        public override bool IsReadRequest => true;
    }
}