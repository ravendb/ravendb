using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;

namespace Raven.Client.Documents.Queries.Facets
{
    public interface IAggregationAsyncDocumentQuery<T>
    {
        IAggregationAsyncDocumentQuery<T> AndAggregateBy(string path, Action<FacetFactory<T>> factory = null);
        IAggregationAsyncDocumentQuery<T> AndAggregateBy(Facet facet);
        Task<Dictionary<string, FacetResult>> ExecuteAsync();
        Lazy<Task<Dictionary<string, FacetResult>>> ExecuteLazyAsync();
    }
}
