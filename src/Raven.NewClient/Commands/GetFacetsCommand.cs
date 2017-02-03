using System;
using System.Net.Http;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    public class GetFacetsCommand : RavenCommand<FacetedQueryResult>
    {
        private readonly JsonOperationContext _context;
        private readonly FacetQuery _query;

        public GetFacetsCommand(JsonOperationContext context, FacetQuery query)
        {
            if (context == null)
                throw new ArgumentNullException(nameof(context));
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            _context = context;
            _query = query;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            if (string.IsNullOrWhiteSpace(_query.FacetSetupDoc) == false && _query.Facets != null && _query.Facets.Count > 0)
                throw new InvalidOperationException($"You cannot specify both '{nameof(FacetQuery.FacetSetupDoc)}' and '{nameof(FacetQuery.Facets)}'.");

            //TODO - EFRAT
            var method = _query.CalculateHttpMethod();

            var request = new HttpRequestMessage
            {
                Method = method
            };

            if (method == HttpMethod.Post)
            {
                request.Content = new BlittableJsonContent(stream =>
                {
                    using (var writer = new BlittableJsonTextWriter(_context, stream))
                    {
                        _context.Write(writer, _query.GetFacetsAsJson());
                    }
                });
            }
            url = $"{node.Url}/databases/{node.Database}/queries/{_query.IndexName}?{_query.GetQueryString(method)}";
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