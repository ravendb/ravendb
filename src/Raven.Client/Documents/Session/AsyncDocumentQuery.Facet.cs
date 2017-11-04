using System;
using System.Collections.Generic;
using Raven.Client.Documents.Queries.Facets;

namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentQuery<T>
    {
        public IAggregationAsyncDocumentQuery<T> AggregateBy(string fieldName, Action<FacetFactory<T>> factory = null)
        {
            var ff = new FacetFactory<T>(fieldName);
            factory?.Invoke(ff);

            return AggregateBy(ff.Facet);
        }

        public new IAggregationAsyncDocumentQuery<T> AggregateBy(Facet facet)
        {
            base.AggregateBy(facet);

            return new AggregationAsyncDocumentQuery<T>(this);
        }

        public IAggregationAsyncDocumentQuery<T> AggregateBy(IEnumerable<Facet> facets)
        {
            foreach (var facet in facets)
                base.AggregateBy(facet);

            return new AggregationAsyncDocumentQuery<T>(this);
        }

        public new IAggregationAsyncDocumentQuery<T> AggregateUsing(string facetSetupDocumentKey)
        {
            base.AggregateUsing(facetSetupDocumentKey);

            return new AggregationAsyncDocumentQuery<T>(this);
        }
    }
}
