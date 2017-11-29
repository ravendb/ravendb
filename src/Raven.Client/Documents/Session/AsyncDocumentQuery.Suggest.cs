using System;
using Raven.Client.Documents.Queries.Suggestion;

namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentQuery<T>
    {
        IAsyncSuggestionDocumentQuery<T> IAsyncDocumentQuery<T>.Suggest(SuggestionBase suggestion)
        {
            Suggest(suggestion);
            return new AsyncSuggestionDocumentQuery<T>(this);
        }

        IAsyncSuggestionDocumentQuery<T> IAsyncDocumentQuery<T>.Suggest(Action<ISuggestionFactory<T>> factory)        {
            var f = new SuggestionFactory<T>();
            factory.Invoke(f);

            Suggest(f.Suggestion);

            return new AsyncSuggestionDocumentQuery<T>(this);        }    }
}
