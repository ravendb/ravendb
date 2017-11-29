using System;
using Raven.Client.Documents.Queries.Suggestions;

namespace Raven.Client.Documents.Session
{
    public partial class DocumentQuery<T>
    {
        ISuggestionDocumentQuery<T> IDocumentQuery<T>.SuggestUsing(SuggestionBase suggestion)
        {
            SuggestUsing(suggestion);
            return new SuggestionDocumentQuery<T>(this);
        }

        ISuggestionDocumentQuery<T> IDocumentQuery<T>.SuggestUsing(Action<ISuggestionFactory<T>> factory)
        {
            var f = new SuggestionFactory<T>();
            factory.Invoke(f);

            SuggestUsing(f.Suggestion);

            return new SuggestionDocumentQuery<T>(this);
        }
    }
}
