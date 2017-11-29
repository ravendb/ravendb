using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Queries.Suggestions
{
    public interface ISuggestionQuery<T>
    {
        Dictionary<string, SuggestionResult> Execute();
        Task<Dictionary<string, SuggestionResult>> ExecuteAsync();
        Lazy<Dictionary<string, SuggestionResult>> ExecuteLazy(Action<Dictionary<string, SuggestionResult>> onEval = null);
        Lazy<Task<Dictionary<string, SuggestionResult>>> ExecuteLazyAsync(Action<Dictionary<string, SuggestionResult>> onEval = null);
    }
}
