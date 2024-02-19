using System;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries.Suggestions
{
    /// <inheritdoc cref="SuggestionQueryBase" />
    internal sealed class SuggestionDocumentQuery<T> : SuggestionQueryBase, ISuggestionDocumentQuery<T>
    {
        private readonly DocumentQuery<T> _source;

        /// <inheritdoc />
        public SuggestionDocumentQuery(DocumentQuery<T> source)
            : base((InMemoryDocumentSessionOperations)source.Session)
        {
            _source = source;
        }

        protected override IndexQuery GetIndexQuery(bool isAsync, bool updateAfterQueryExecuted = true)
        {
            return _source.GetIndexQuery();
        }

        protected override void InvokeAfterQueryExecuted(QueryResult result)
        {
            _source.InvokeAfterQueryExecuted(result);
        }

        /// <inheritdoc />
        public ISuggestionDocumentQuery<T> AndSuggestUsing(SuggestionBase suggestion)
        {
            _source.SuggestUsing(suggestion);
            return this;
        }

        /// <inheritdoc />
        public ISuggestionDocumentQuery<T> AndSuggestUsing(Action<ISuggestionBuilder<T>> builder)
        {
            var f = new SuggestionBuilder<T>(_source.Conventions);
            builder.Invoke(f);

            _source.SuggestUsing(f.Suggestion);
            return this;
        }
    }
}
