using System;
using System.Collections.Generic;
using Raven.Client.Documents.Commands;

namespace Raven.Client.Documents.Queries.Facets
{
    public interface IAggregationDocumentQuery<T>
    {
        IAggregationDocumentQuery<T> AndAggregateOn(string path, Action<FacetFactory<T>> factory = null);
        IAggregationDocumentQuery<T> AndAggregateOn(Facet facet);
        Dictionary<string, FacetResult> Execute();
        Lazy<Dictionary<string, FacetResult>> ExecuteLazy();
    }
}
