using System;
using System.Collections.Generic;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries.Facets;

namespace Raven.Client.Documents.Session
{
    public partial class DocumentQuery<T>
    {
        public AggregationQuery<T> AggregateBy(string fieldName, Action<FacetFactory<T>> factory = null)
        {
            throw new NotImplementedException();
        }

        public new AggregationQuery<T> AggregateBy(Facet facet)
        {
            throw new NotImplementedException();
        }

        public AggregationQuery<T> AggregateBy(IEnumerable<Facet> facets)
        {
            throw new NotImplementedException();
        }

        public new AggregationQuery<T> AggregateUsing(string facetSetupDocumentKey)
        {
            throw new NotImplementedException();
        }
    }
}
