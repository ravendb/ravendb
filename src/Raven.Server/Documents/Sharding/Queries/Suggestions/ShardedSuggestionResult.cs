using System.Collections.Generic;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Server.Documents.Indexes.Persistence;

namespace Raven.Server.Documents.Sharding.Queries.Suggestions;

internal sealed class ShardedSuggestionResult : SuggestionResult
{
    public Popularity SuggestionsWithPopularity;

    internal sealed class Popularity
    {
        public List<SuggestWord> Values;
    }
}
