using System.Collections.Generic;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Suggestions;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Queries.Suggestions;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene;

public sealed partial  class LuceneSuggestionIndexReader : SuggestionIndexReaderBase
{
    partial void AddPopularity(SuggestWord suggestion, ref SuggestionResult result)
    {
        if (_index.DocumentDatabase is ShardedDocumentDatabase == false)
            return;

        if (result is SuggestionResultWithPopularity resultWithPopularity)
        {
            resultWithPopularity.SuggestionsPopularity.Add(new SuggestionResultWithPopularity.Popularity
            {
                Freq = suggestion.Freq,
                Score = suggestion.Score
            });
        }
        else
        {
            result = new SuggestionResultWithPopularity
            {
                Name = result.Name,
                Suggestions = result.Suggestions,
                SuggestionsPopularity = new List<SuggestionResultWithPopularity.Popularity>
                {
                    new() {Freq = suggestion.Freq, Score = suggestion.Score}
                }
            };
        }
    }
}
