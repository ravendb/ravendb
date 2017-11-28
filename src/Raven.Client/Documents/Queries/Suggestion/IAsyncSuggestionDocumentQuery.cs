using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;

namespace Raven.Client.Documents.Queries.Suggestion
{
    public interface IAsyncSuggestionDocumentQuery<T>
    {
        Task<Dictionary<string, SuggestionResult>> ExecuteAsync();
        Lazy<Task<Dictionary<string, SuggestionResult>>> ExecuteLazyAsync(Action<string[]> onEval = null);
    }
}
