using System;
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

        public ISuggestionDocumentQuery<T> AndSuggestUsing(SuggestionBase suggestion)
        {
            _source.SuggestUsing(suggestion);
            return this;
        }

        public ISuggestionDocumentQuery<T> AndSuggestUsing(Action<ISuggestionBuilder<T>> builder)
        {
            var f = new SuggestionBuilder<T>();
            builder.Invoke(f);

            _source.SuggestUsing(f.Suggestion);
            return this;
        }
    }
}
