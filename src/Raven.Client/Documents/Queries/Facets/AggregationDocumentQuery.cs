using System;
using Raven.Client.Documents.Linq;
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

        public IAggregationDocumentQuery<T> AndAggregateOn(string path, Action<FacetFactory<T>> factory = null)
        {
            var f = new FacetFactory<T>(path);
            factory?.Invoke(f);

            return AndAggregateOn(f.Facet);
        }

        public IAggregationDocumentQuery<T> AndAggregateOn(Facet facet)
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
