using System.Collections.Generic;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries;

namespace Raven.Server.Documents.Queries.Suggestion
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
