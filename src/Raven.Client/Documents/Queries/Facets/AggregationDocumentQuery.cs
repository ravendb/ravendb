using System;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries.Facets
{
    internal class AggregationDocumentQuery<T> : AggregationQueryBase, IAggregationDocumentQuery<T>
    {
        private readonly AbstractDocumentQuery<T, DocumentQuery<T>> _source;

        public AggregationDocumentQuery(DocumentQuery<T> source) : base((InMemoryDocumentSessionOperations)source.Session)
        {
            _source = source;
        }

        public IAggregationDocumentQuery<T> AndAggregateBy(Action<IFacetBuilder<T>> builder = null)
        {
            var f = new FacetBuilder<T>();
            builder?.Invoke(f);

            return AndAggregateBy(f.Facet);
        }

        public IAggregationDocumentQuery<T> AndAggregateBy(FacetBase facet)
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
