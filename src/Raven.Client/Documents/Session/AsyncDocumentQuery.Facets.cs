using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Queries.Facets;

namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentQuery<T>
    {
        public IAsyncAggregationDocumentQuery<T> AggregateBy(Action<IFacetBuilder<T>> builder)
        {
            var ff = new FacetBuilder<T>();
            builder.Invoke(ff);

            return AggregateBy(ff.Facet);
        }

        public new IAsyncAggregationDocumentQuery<T> AggregateBy(FacetBase facet)
        {
            base.AggregateBy(facet);

            return new AsyncAggregationDocumentQuery<T>(this);
        }

        public IAsyncAggregationDocumentQuery<T> AggregateBy(IEnumerable<FacetBase> facets)
        {
            foreach (var facet in facets)
                base.AggregateBy(facet);

            return new AsyncAggregationDocumentQuery<T>(this);
        }

        public new IAsyncAggregationDocumentQuery<T> AggregateUsing(string facetSetupDocumentId)
        {
            base.AggregateUsing(facetSetupDocumentId);

            return new AsyncAggregationDocumentQuery<T>(this);
        }

        public Task<Dictionary<string, FacetResult>> ExecuteAggregationAsync(CancellationToken token)
        {
            var query = new AsyncAggregationRawDocumentQuery<T>(this, AsyncSession);
            return query.ExecuteAsync(token);
        }
    }
}
