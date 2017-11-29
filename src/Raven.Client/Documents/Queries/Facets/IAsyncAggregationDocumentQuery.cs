using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Queries.Facets
{
    public interface IAsyncAggregationDocumentQuery<T>
    {
        IAsyncAggregationDocumentQuery<T> AndAggregateBy(Action<IFacetFactory<T>> factory = null);
        IAsyncAggregationDocumentQuery<T> AndAggregateBy(FacetBase facet);
        Task<Dictionary<string, FacetResult>> ExecuteAsync();
        Lazy<Task<Dictionary<string, FacetResult>>> ExecuteLazyAsync(Action<Dictionary<string, FacetResult>> onEval = null);
    }
}
