using System.Collections.Generic;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Suggestions;

namespace Raven.Server.Documents.Sharding.Queries.Suggestions;

internal class SuggestionResultWithPopularity : SuggestionResult
{
    public Popularity SuggestionsWithPopularity;

    internal class Popularity
    {
        public List<SuggestWord> Values;
    }
}
