using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Queries.Facets
{
    public interface IAggregationQuery<T>
    {
        IAggregationQuery<T> AndAggregateBy(Action<IFacetBuilder<T>> builder = null);
        IAggregationQuery<T> AndAggregateBy(FacetBase facet);
        Dictionary<string, FacetResult> Execute();
        Task<Dictionary<string, FacetResult>> ExecuteAsync(CancellationToken token = default);
        Lazy<Dictionary<string, FacetResult>> ExecuteLazy(Action<Dictionary<string, FacetResult>> onEval = null);
        Lazy<Task<Dictionary<string, FacetResult>>> ExecuteLazyAsync(Action<Dictionary<string, FacetResult>> onEval = null, CancellationToken token = default);
    }
}
