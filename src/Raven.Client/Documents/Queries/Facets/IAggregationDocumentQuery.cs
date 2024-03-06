using System;
using System.Collections.Generic;

namespace Raven.Client.Documents.Queries.Facets
{
    public interface IAggregationDocumentQuery<T>
    {
        /// <inheritdoc cref="IAggregationQuery{T}.AndAggregateBy(System.Action{Raven.Client.Documents.Queries.Facets.IFacetBuilder{T}})"/>
        IAggregationDocumentQuery<T> AndAggregateBy(Action<IFacetBuilder<T>> builder = null);

        /// <inheritdoc cref="IAggregationQuery{T}.AndAggregateBy(FacetBase)"/>
        IAggregationDocumentQuery<T> AndAggregateBy(FacetBase facet);

        Dictionary<string, FacetResult> Execute();
        Lazy<Dictionary<string, FacetResult>> ExecuteLazy(Action<Dictionary<string, FacetResult>> onEval = null);
    }
}
