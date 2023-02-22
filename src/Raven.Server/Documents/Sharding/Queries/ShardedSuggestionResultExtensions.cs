using System.Collections.Generic;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Sharding.Queries.Suggestions;

namespace Raven.Server.Documents.Sharding.Queries;

public static class ShardedSuggestionResultExtensions
{
    internal static ShardedSuggestionResult AddPopularity(this SuggestionResult result, SuggestWord suggestion)
    {
        if (result is ShardedSuggestionResult resultWithPopularity)
        {
            resultWithPopularity.SuggestionsWithPopularity.Values.Add(suggestion);
        }
        else
        {
            resultWithPopularity = new ShardedSuggestionResult
            {
                Name = result.Name,
                Suggestions = result.Suggestions,
                SuggestionsWithPopularity = new ShardedSuggestionResult.Popularity
                {
                    Values = new List<SuggestWord>
                    {
                        suggestion
                    }
                }
            };
        }

        return resultWithPopularity;
    }
}
