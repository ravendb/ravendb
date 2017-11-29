using System;
using Raven.Client.Documents.Queries.Suggestions;

namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentQuery<T>
    {
        IAsyncSuggestionDocumentQuery<T> IAsyncDocumentQuery<T>.SuggestUsing(SuggestionBase suggestion)
        {
            SuggestUsing(suggestion);
            return new AsyncSuggestionDocumentQuery<T>(this);
        }

        IAsyncSuggestionDocumentQuery<T> IAsyncDocumentQuery<T>.SuggestUsing(Action<ISuggestionFactory<T>> factory)
        {
            var f = new SuggestionFactory<T>();
            factory.Invoke(f);

            SuggestUsing(f.Suggestion);

            return new AsyncSuggestionDocumentQuery<T>(this);
        }
    }
}
