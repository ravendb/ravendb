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

        ISuggestionDocumentQuery<T> IDocumentQuery<T>.SuggestUsing(Action<ISuggestionBuilder<T>> builder)
        {
            var f = new SuggestionBuilder<T>();
            builder.Invoke(f);

            SuggestUsing(f.Suggestion);

            return new SuggestionDocumentQuery<T>(this);
        }
    }
}
