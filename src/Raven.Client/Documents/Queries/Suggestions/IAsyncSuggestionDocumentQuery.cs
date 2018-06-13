using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Queries.Suggestions
{
    public interface IAsyncSuggestionDocumentQuery<T>
    {
        Task<Dictionary<string, SuggestionResult>> ExecuteAsync(CancellationToken token = default);
        Lazy<Task<Dictionary<string, SuggestionResult>>> ExecuteLazyAsync(Action<Dictionary<string, SuggestionResult>> onEval = null, CancellationToken token = default);
    }
}
