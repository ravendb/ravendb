using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries.Suggestions
{
    internal class AsyncSuggestionDocumentQuery<T> : SuggestionQueryBase, IAsyncSuggestionDocumentQuery<T>
    {
        private readonly AsyncDocumentQuery<T> _source;

        public AsyncSuggestionDocumentQuery(AsyncDocumentQuery<T> source)
            : base((InMemoryDocumentSessionOperations)source.AsyncSession)
        {
            _source = source;
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
