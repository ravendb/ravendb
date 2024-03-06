using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Queries.Facets
{
    public interface IAggregationQuery<T>
    {
        /// <summary>
        /// A Faceted Search provides an efficient way to explore and navigate through large datasets or search results.
        /// Multiple filters(facets) are applied to narrow down the search results according to different attributes or categories.
        /// Aggregation of data is available for an index-field per unique Facet or Range item.
        /// </summary>
        /// <param name="builder">Builder of aggregation query.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.AggregationQuery"/>
        IAggregationQuery<T> AndAggregateBy(Action<IFacetBuilder<T>> builder = null);

        /// <summary>
        /// A Faceted Search provides an efficient way to explore and navigate through large datasets or search results.
        /// Multiple filters(facets) are applied to narrow down the search results according to different attributes or categories.
        /// </summary>
        /// <param name="facet"><see cref="FacetBase"/> is used to pass aggregation query.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.AggregationQuery"/>
        IAggregationQuery<T> AndAggregateBy(FacetBase facet);
        
        Dictionary<string, FacetResult> Execute();
        Task<Dictionary<string, FacetResult>> ExecuteAsync(CancellationToken token = default);
        Lazy<Dictionary<string, FacetResult>> ExecuteLazy(Action<Dictionary<string, FacetResult>> onEval = null);
        Lazy<Task<Dictionary<string, FacetResult>>> ExecuteLazyAsync(Action<Dictionary<string, FacetResult>> onEval = null, CancellationToken token = default);
    }
}
