using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;

namespace Raven.Client.Documents.Queries.Facets
{
    public interface IAggregationQuery<T>
    {
        IAggregationQuery<T> AndAggregateOn(Expression<Func<T, object>> path, Action<FacetFactory<T>> factory = null);
        IAggregationQuery<T> AndAggregateOn(string path, Action<FacetFactory<T>> factory = null);
        IAggregationQuery<T> AndAggregateOn(Facet facet);
        Dictionary<string, FacetResult> Execute();
        Task<Dictionary<string, FacetResult>> ExecuteAsync();
        Lazy<Dictionary<string, FacetResult>> ExecuteLazy();
        Lazy<Task<Dictionary<string, FacetResult>>> ExecuteLazyAsync();
    }
}