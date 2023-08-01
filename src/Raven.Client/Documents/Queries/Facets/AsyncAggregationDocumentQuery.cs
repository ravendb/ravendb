using System;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries.Facets
{
    internal sealed class AsyncAggregationDocumentQuery<T> : AggregationQueryBase, IAsyncAggregationDocumentQuery<T>
    {
        private readonly AbstractDocumentQuery<T, AsyncDocumentQuery<T>> _source;

        public AsyncAggregationDocumentQuery(AsyncDocumentQuery<T> source) : base((InMemoryDocumentSessionOperations)source.AsyncSession)
        {
            _source = source;
        }

        public IAsyncAggregationDocumentQuery<T> AndAggregateBy(Action<IFacetBuilder<T>> builder = null)
        {
            var f = new FacetBuilder<T>(_source.Conventions);
            builder?.Invoke(f);

            return AndAggregateBy(f.Facet);
        }

        public IAsyncAggregationDocumentQuery<T> AndAggregateBy(FacetBase facet)
        {
            _source.AggregateBy(facet);
            return this;
        }

        protected override IndexQuery GetIndexQuery(bool isAsync, bool updateAfterQueryExecuted = true)
        {
            return _source.GetIndexQuery();
        }

        protected override void InvokeAfterQueryExecuted(QueryResult result)
        {
            _source.InvokeAfterQueryExecuted(result);
        }
    }
}
