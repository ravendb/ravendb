using System;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class GetFacetsOperation : IOperation<FacetedQueryResult>
    {
        private readonly FacetQuery _query;

        public GetFacetsOperation(FacetQuery query)
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            _query = query;
        }

        public RavenCommand<FacetedQueryResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetFacetsCommand(conventions, context, _query);
        }
    }
}