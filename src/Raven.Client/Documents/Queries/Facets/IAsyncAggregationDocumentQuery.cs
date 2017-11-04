using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;

namespace Raven.Client.Documents.Queries.Facets
{
    public interface IAsyncAggregationDocumentQuery<T>
    {
        IAsyncAggregationDocumentQuery<T> AndAggregateBy(string path, Action<FacetFactory<T>> factory = null);
        IAsyncAggregationDocumentQuery<T> AndAggregateBy(Facet facet);
        Task<Dictionary<string, FacetResult>> ExecuteAsync();
        Lazy<Task<Dictionary<string, FacetResult>>> ExecuteLazyAsync();
    }
}
