using System;
using System.Collections.Generic;
using Raven.Client.Documents.Commands;

namespace Raven.Client.Documents.Queries.Suggestion
{
    public interface ISuggestionDocumentQuery<T>
    {
        Dictionary<string, SuggestionResult> Execute();
        Lazy<Dictionary<string, SuggestionResult>> ExecuteLazy(Action<string[]> onEval = null);
    }
}
