using System;
using Raven.Client.Documents.Queries.Suggestions;

namespace Raven.Client.Documents.Session
{
    public partial class DocumentQuery<T>
    {
        ISuggestionDocumentQuery<T> IDocumentQuery<T>.Suggest(SuggestionBase suggestion)
        {
            Suggest(suggestion);
            return new SuggestionDocumentQuery<T>(this);
        }

        ISuggestionDocumentQuery<T> IDocumentQuery<T>.Suggest(Action<ISuggestionFactory<T>> factory)
        {
            var f = new SuggestionFactory<T>();
            factory.Invoke(f);

            Suggest(f.Suggestion);

            return new SuggestionDocumentQuery<T>(this);
        }
    }
}
