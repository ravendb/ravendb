using System.Collections.Generic;
using Raven.Client.Documents.Queries.Suggestions;

namespace Raven.Server.Documents.Sharding.Queries.Suggestions;

public class SuggestionResultWithPopularity : SuggestionResult
{
    public List<Popularity> SuggestionsPopularity;

    public class Popularity
    {
        public float Score;
        
        public int Freq;
    }
}
