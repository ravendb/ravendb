using System;
using System.Collections.Generic;
using Raven.Client.Documents.Queries.Facets;

namespace Raven.Client.Documents.Session
{
    public partial class DocumentQuery<T>
    {
        public IAggregationDocumentQuery<T> AggregateBy(Action<IFacetBuilder<T>> builder)
        {
            var ff = new FacetBuilder<T>();
            builder.Invoke(ff);

            return AggregateBy(ff.Facet);
        }

        public new IAggregationDocumentQuery<T> AggregateBy(FacetBase facet)
        {
            base.AggregateBy(facet);

            return new AggregationDocumentQuery<T>(this);
        }

        public IAggregationDocumentQuery<T> AggregateBy(IEnumerable<FacetBase> facets)
        {
            foreach (var facet in facets)
                base.AggregateBy(facet);

            return new AggregationDocumentQuery<T>(this);
        }

        public new IAggregationDocumentQuery<T> AggregateUsing(string facetSetupDocumentId)
        {
            base.AggregateUsing(facetSetupDocumentId);

            return new AggregationDocumentQuery<T>(this);
        }
    }
}
