using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetFacetsCommand : RavenCommand<FacetedQueryResult>
    {
        private readonly DocumentConventions _conventions;
        private readonly JsonOperationContext _context;
        private readonly FacetQuery _query;

        public GetFacetsCommand(DocumentConventions conventions, JsonOperationContext context, FacetQuery query)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _context = context ?? throw new ArgumentNullException(nameof(context));
            _query = query ?? throw new ArgumentNullException(nameof(query));

            if (_query.WaitForNonStaleResultsTimeout.HasValue && _query.WaitForNonStaleResultsTimeout != TimeSpan.MaxValue)
                Timeout = _query.WaitForNonStaleResultsTimeout.Value.Add(TimeSpan.FromSeconds(10)); // giving the server an opportunity to finish the response
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            if (string.IsNullOrWhiteSpace(_query.FacetSetupDoc) == false && _query.Facets != null && _query.Facets.Count > 0)
                throw new InvalidOperationException($"You cannot specify both '{nameof(FacetQuery.FacetSetupDoc)}' and '{nameof(FacetQuery.Facets)}'.");

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(stream =>
                {
                    using (var writer = new BlittableJsonTextWriter(_context, stream))
                    {
                        writer.WriteFacetQuery(_conventions, _context, _query);
                    }
                })
            };

            url = $"{node.Url}/databases/{node.Database}/queries?op=facets";
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