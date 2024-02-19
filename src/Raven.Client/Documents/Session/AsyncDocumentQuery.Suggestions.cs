using System;
using Raven.Client.Documents.Queries.Suggestions;

namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentQuery<T>
    {
        /// <inheritdoc />
        IAsyncSuggestionDocumentQuery<T> IAsyncDocumentQuery<T>.SuggestUsing(SuggestionBase suggestion)
        {
            SuggestUsing(suggestion);
            return new AsyncSuggestionDocumentQuery<T>(this);
        }

        /// <inheritdoc />
        IAsyncSuggestionDocumentQuery<T> IAsyncDocumentQuery<T>.SuggestUsing(Action<ISuggestionBuilder<T>> builder)
        {
            var f = new SuggestionBuilder<T>(Conventions);
            builder.Invoke(f);

            SuggestUsing(f.Suggestion);

            return new AsyncSuggestionDocumentQuery<T>(this);
        }
    }
}
