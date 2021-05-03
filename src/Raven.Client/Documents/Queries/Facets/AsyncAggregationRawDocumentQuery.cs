using System;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries.Facets
{
    internal class AsyncAggregationRawDocumentQuery<T> : AggregationQueryBase
    {
        private readonly IAsyncRawDocumentQuery<T> _source;

        public AsyncAggregationRawDocumentQuery(IAsyncRawDocumentQuery<T> source, IAsyncDocumentSession session)
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
