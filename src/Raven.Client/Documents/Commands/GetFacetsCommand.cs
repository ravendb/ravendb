using System;
using System.Net.Http;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetFacetsCommand : RavenCommand<FacetedQueryResult>
    {
        private readonly JsonOperationContext _context;
        private readonly FacetQuery _query;

        public GetFacetsCommand(JsonOperationContext context, FacetQuery query)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
            _query = query ?? throw new ArgumentNullException(nameof(query));

            if (_query.WaitForNonStaleResultsTimeout.HasValue && _query.WaitForNonStaleResultsTimeout != TimeSpan.MaxValue)
                Timeout = _query.WaitForNonStaleResultsTimeout.Value.Add(TimeSpan.FromSeconds(10)); // giving the server an opportunity to finish the response
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