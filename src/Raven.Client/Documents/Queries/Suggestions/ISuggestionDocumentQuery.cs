using System;
using System.Collections.Generic;

namespace Raven.Client.Documents.Queries.Suggestions
{
    /// <inheritdoc cref="ISuggestionQuery{T}"/>
    public interface ISuggestionDocumentQuery<T>
    {
        /// <inheritdoc cref="ISuggestionQuery{T}.AndSuggestUsing(Raven.Client.Documents.Queries.Suggestions.SuggestionBase)"/>
        ISuggestionDocumentQuery<T> AndSuggestUsing(SuggestionBase suggestion);

        /// <inheritdoc cref="ISuggestionQuery{T}.AndSuggestUsing(Action{ISuggestionBuilder{T}})"/>
        ISuggestionDocumentQuery<T> AndSuggestUsing(Action<ISuggestionBuilder<T>> builder);

        Dictionary<string, SuggestionResult> Execute();
        Lazy<Dictionary<string, SuggestionResult>> ExecuteLazy(Action<Dictionary<string, SuggestionResult>> onEval = null);
    }
}
