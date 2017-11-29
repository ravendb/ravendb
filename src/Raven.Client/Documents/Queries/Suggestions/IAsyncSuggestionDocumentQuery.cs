using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Queries.Suggestions
{
    public interface IAsyncSuggestionDocumentQuery<T>
    {
        Task<Dictionary<string, SuggestionResult>> ExecuteAsync();
        Lazy<Task<Dictionary<string, SuggestionResult>>> ExecuteLazyAsync(Action<Dictionary<string, SuggestionResult>> onEval = null);
    }
}
