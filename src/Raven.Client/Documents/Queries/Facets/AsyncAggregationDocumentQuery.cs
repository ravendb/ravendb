using System;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries.Facets
{
    internal class AsyncAggregationDocumentQuery<T> : AggregationQueryBase, IAsyncAggregationDocumentQuery<T>
    {
        private readonly AbstractDocumentQuery<T, AsyncDocumentQuery<T>> _source;

        public AsyncAggregationDocumentQuery(AsyncDocumentQuery<T> source) : base((InMemoryDocumentSessionOperations)source.AsyncSession)
        {
            _source = source;
        }

        public IAsyncAggregationDocumentQuery<T> AndAggregateBy(string path, Action<FacetFactory<T>> factory = null)
        {
            var f = new FacetFactory<T>(path);
            factory?.Invoke(f);

            return AndAggregateBy(f.Facet);
        }

        public IAsyncAggregationDocumentQuery<T> AndAggregateBy(Facet facet)
        {
            _source.AggregateBy(facet);
            return this;
        }

        protected override IndexQuery GetIndexQuery(bool isAsync)
        {
            return _source.GetIndexQuery();
        }

        protected override void InvokeAfterQueryExecuted(QueryResult result)
        {
            _source.InvokeAfterQueryExecuted(result);
        }
    }
}
