using System.Collections.Generic;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Suggestions;
using Raven.Server.Documents.Sharding.Queries.Suggestions;
using Raven.Server.Indexing;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Sharding.Persistence.Lucene;

public sealed class ShardedLuceneSuggestionIndexReader : LuceneSuggestionIndexReader
{
    public ShardedLuceneSuggestionIndexReader(Index index, LuceneVoronDirectory directory, LuceneIndexSearcherHolder searcherHolder, Transaction readTransaction) : base(index, directory, searcherHolder, readTransaction)
    {
    }

    internal override void AddPopularity(SuggestWord suggestion, ref SuggestionResult result)
    {
        if (result is ShardedSuggestionResult resultWithPopularity)
        {
            resultWithPopularity.SuggestionsWithPopularity.Values.Add(suggestion);
        }
        else
        {
            result = new ShardedSuggestionResult
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
    }
}
