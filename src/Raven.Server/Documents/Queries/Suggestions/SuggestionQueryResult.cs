using System.Collections.Generic;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Suggestions;

namespace Raven.Server.Documents.Queries.Suggestions
{
    public class SuggestionQueryResult : QueryResult<List<SuggestionResult>, List<Document>>
    {
        public static readonly SuggestionQueryResult NotModifiedResult = new SuggestionQueryResult { NotModified = true };

        public SuggestionQueryResult()
        {
            Results = new List<SuggestionResult>();
            Includes = new List<Document>();
        }

        public bool NotModified { get; private set; }
    }
}
