using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Queries.Suggestions
{
    /// <inheritdoc cref="ISuggestionDocumentQuery{T}"/>
    public interface IAsyncSuggestionDocumentQuery<T>
    {
        /// <inheritdoc cref="ISuggestionDocumentQuery{T}.AndSuggestUsing(Raven.Client.Documents.Queries.Suggestions.SuggestionBase)"/>
        IAsyncSuggestionDocumentQuery<T> AndSuggestUsing(SuggestionBase suggestion);

        /// <inheritdoc cref="ISuggestionQuery{T}.AndSuggestUsing(Action{ISuggestionBuilder{T}})"/>

        IAsyncSuggestionDocumentQuery<T> AndSuggestUsing(Action<ISuggestionBuilder<T>> builder);

        Task<Dictionary<string, SuggestionResult>> ExecuteAsync(CancellationToken token = default);
        Lazy<Task<Dictionary<string, SuggestionResult>>> ExecuteLazyAsync(Action<Dictionary<string, SuggestionResult>> onEval = null, CancellationToken token = default);
    }
}
