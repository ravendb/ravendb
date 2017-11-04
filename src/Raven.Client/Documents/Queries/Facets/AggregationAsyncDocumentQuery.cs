using System;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries.Facets
{
    internal class AggregationAsyncDocumentQuery<T> : AggregationQueryBase, IAggregationAsyncDocumentQuery<T>
    {
        private readonly AbstractDocumentQuery<T, AsyncDocumentQuery<T>> _source;

        public AggregationAsyncDocumentQuery(AsyncDocumentQuery<T> source) : base((InMemoryDocumentSessionOperations)source.AsyncSession)
        {
            _source = source;
        }

        public IAggregationAsyncDocumentQuery<T> AndAggregateOn(string path, Action<FacetFactory<T>> factory = null)
        {
            var f = new FacetFactory<T>(path);
            factory?.Invoke(f);

            return AndAggregateOn(f.Facet);
        }

        public IAggregationAsyncDocumentQuery<T> AndAggregateOn(Facet facet)
        {
            _source.AggregateBy(facet);
            return this;
        }

        protected override IndexQuery GetIndexQuery(bool isAsync)
        {
            return _source.GetIndexQuery();
        }
    }
}
