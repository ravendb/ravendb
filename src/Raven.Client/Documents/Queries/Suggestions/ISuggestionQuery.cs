using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Queries.Suggestions
{
    public interface ISuggestionQuery<T>
    {
        ISuggestionQuery<T> AndSuggestUsing(SuggestionBase suggestion);
        ISuggestionQuery<T> AndSuggestUsing(Action<ISuggestionBuilder<T>> builder);

        Dictionary<string, SuggestionResult> Execute();
        Task<Dictionary<string, SuggestionResult>> ExecuteAsync(CancellationToken token = default);
        Lazy<Dictionary<string, SuggestionResult>> ExecuteLazy(Action<Dictionary<string, SuggestionResult>> onEval = null);
        Lazy<Task<Dictionary<string, SuggestionResult>>> ExecuteLazyAsync(Action<Dictionary<string, SuggestionResult>> onEval = null, CancellationToken token = default);
    }
}
