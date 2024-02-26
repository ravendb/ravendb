using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Queries.Suggestions
{
    /// <inheritdoc cref="SuggestionBase"/>
    public interface ISuggestionQuery<T>
    {
        /// <inheritdoc cref="ISuggestionQuery{T}"/>
        /// <param name="suggestion">Configure suggestion clause in your query.</param>
        ISuggestionQuery<T> AndSuggestUsing(SuggestionBase suggestion);

        /// <inheritdoc cref="ISuggestionQuery{T}"/>
        /// <param name="builder">Configure suggestion clause in your query by builder.</param>
        /// <returns></returns>
        ISuggestionQuery<T> AndSuggestUsing(Action<ISuggestionBuilder<T>> builder);

        Dictionary<string, SuggestionResult> Execute();

        Task<Dictionary<string, SuggestionResult>> ExecuteAsync(CancellationToken token = default);
        
        Lazy<Dictionary<string, SuggestionResult>> ExecuteLazy(Action<Dictionary<string, SuggestionResult>> onEval = null);
        
        Lazy<Task<Dictionary<string, SuggestionResult>>> ExecuteLazyAsync(Action<Dictionary<string, SuggestionResult>> onEval = null, CancellationToken token = default);
    }
}
