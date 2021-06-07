using System;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries.Facets
{
    internal class AggregationRawDocumentQuery<T> : AggregationQueryBase
    {
        private readonly IRawDocumentQuery<T> _source;

        public AggregationRawDocumentQuery(IRawDocumentQuery<T> source, IDocumentSession session)
            : base((InMemoryDocumentSessionOperations)session)
        {
            _source = source ?? throw new ArgumentNullException(nameof(source));
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
