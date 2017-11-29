using System;
using System.Collections.Generic;

namespace Raven.Client.Documents.Queries.Suggestions
{
    public interface ISuggestionDocumentQuery<T>
    {
        Dictionary<string, SuggestionResult> Execute();
        Lazy<Dictionary<string, SuggestionResult>> ExecuteLazy(Action<Dictionary<string, SuggestionResult>> onEval = null);
    }
}
