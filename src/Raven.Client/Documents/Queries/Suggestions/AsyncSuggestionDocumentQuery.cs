using System;
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

        public IAsyncSuggestionDocumentQuery<T> AndSuggestUsing(SuggestionBase suggestion)
        {
            _source.SuggestUsing(suggestion);
            return this;
        }

        public IAsyncSuggestionDocumentQuery<T> AndSuggestUsing(Action<ISuggestionBuilder<T>> builder)
        {
            var f = new SuggestionBuilder<T>();
            builder.Invoke(f);

            _source.SuggestUsing(f.Suggestion);
            return this;
        }
    }
}
