using System;
using System.Collections.Generic;

namespace Raven.Client.Documents.Queries.Suggestions
{
    public interface ISuggestionDocumentQuery<T>
    {
        ISuggestionDocumentQuery<T> AndSuggestUsing(SuggestionBase suggestion);
        ISuggestionDocumentQuery<T> AndSuggestUsing(Action<ISuggestionBuilder<T>> builder);

        Dictionary<string, SuggestionResult> Execute();
        Lazy<Dictionary<string, SuggestionResult>> ExecuteLazy(Action<Dictionary<string, SuggestionResult>> onEval = null);
    }
}
