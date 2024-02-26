using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Queries.Facets
{
    public interface IAsyncAggregationDocumentQuery<T>
    {
        /// <inheritdoc cref="IAggregationQuery{T}.AndAggregateBy(System.Action{Raven.Client.Documents.Queries.Facets.IFacetBuilder{T}})"/>
        IAsyncAggregationDocumentQuery<T> AndAggregateBy(Action<IFacetBuilder<T>> builder = null);

        /// <inheritdoc cref="IAggregationQuery{T}.AndAggregateBy(FacetBase)"/>
        IAsyncAggregationDocumentQuery<T> AndAggregateBy(FacetBase facet);
        
        Task<Dictionary<string, FacetResult>> ExecuteAsync(CancellationToken token = default);
        Lazy<Task<Dictionary<string, FacetResult>>> ExecuteLazyAsync(Action<Dictionary<string, FacetResult>> onEval = null, CancellationToken token = default);
    }
}
