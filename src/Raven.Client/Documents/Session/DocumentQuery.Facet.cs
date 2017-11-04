using System;
using System.Collections.Generic;
using Raven.Client.Documents.Queries.Facets;

namespace Raven.Client.Documents.Session
{
    public partial class DocumentQuery<T>
    {
        public IAggregationDocumentQuery<T> AggregateBy(string fieldName, Action<FacetFactory<T>> factory = null)
        {
            var ff = new FacetFactory<T>(fieldName);
            factory?.Invoke(ff);

            return AggregateBy(ff.Facet);
        }

        public new IAggregationDocumentQuery<T> AggregateBy(Facet facet)
        {
            base.AggregateBy(facet);

            return new AggregationDocumentQuery<T>(this);
        }

        public IAggregationDocumentQuery<T> AggregateBy(IEnumerable<Facet> facets)
        {
            foreach (var facet in facets)
                base.AggregateBy(facet);

            return new AggregationDocumentQuery<T>(this);
        }

        public new IAggregationDocumentQuery<T> AggregateUsing(string facetSetupDocumentKey)
        {
            base.AggregateUsing(facetSetupDocumentKey);

            return new AggregationDocumentQuery<T>(this);
        }
    }
}
