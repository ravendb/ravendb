using System;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session.Tokens;

namespace Raven.Client.Documents.Session
{
    public abstract partial class AbstractDocumentQuery<T, TSelf>
    {
        public void AggregateBy(FacetBase facet)
        {
            foreach (var token in SelectTokens)
            {
                if (token is FacetToken)
                    continue;

                throw new InvalidOperationException($"Aggregation query can select only facets while it got {token.GetType().Name} token");
            }

            SelectTokens.AddLast(FacetToken.Create(facet, AddQueryParameter));
        }

        public void AggregateUsing(string facetSetupDocumentId)
        {
            SelectTokens.AddLast(FacetToken.Create(facetSetupDocumentId));
        }
    }
}
