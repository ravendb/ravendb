using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Queries.Suggestions
{
    public interface IAsyncSuggestionDocumentQuery<T>
    {
        IAsyncSuggestionDocumentQuery<T> AndSuggestUsing(SuggestionBase suggestion);
        IAsyncSuggestionDocumentQuery<T> AndSuggestUsing(Action<ISuggestionBuilder<T>> builder);

        Task<Dictionary<string, SuggestionResult>> ExecuteAsync(CancellationToken token = default);
        Lazy<Task<Dictionary<string, SuggestionResult>>> ExecuteLazyAsync(Action<Dictionary<string, SuggestionResult>> onEval = null, CancellationToken token = default);
    }
}
