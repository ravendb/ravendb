using System;
using System.Collections.Generic;

namespace Raven.Client.Documents.Queries.Facets
{
    public interface IAggregationDocumentQuery<T>
    {
        IAggregationDocumentQuery<T> AndAggregateBy(Action<IFacetFactory<T>> factory = null);
        IAggregationDocumentQuery<T> AndAggregateBy(FacetBase facet);
        Dictionary<string, FacetResult> Execute();
        Lazy<Dictionary<string, FacetResult>> ExecuteLazy(Action<Dictionary<string, FacetResult>> onEval = null);
    }
}
