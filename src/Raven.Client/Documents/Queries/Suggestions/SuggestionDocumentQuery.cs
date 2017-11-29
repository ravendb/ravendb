using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries.Suggestions
{
    internal class SuggestionDocumentQuery<T> : SuggestionQueryBase, ISuggestionDocumentQuery<T>
    {
        private readonly DocumentQuery<T> _source;

        public SuggestionDocumentQuery(DocumentQuery<T> source)
            : base((InMemoryDocumentSessionOperations)source.Session)
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
