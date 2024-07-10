using Lucene.Net.Search;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Sharding.Queries;
using Raven.Server.Indexing;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Sharding.Persistence.Lucene;

public sealed class ShardedLuceneSuggestionIndexReader : LuceneSuggestionIndexReader
{
    public ShardedLuceneSuggestionIndexReader(Index index, LuceneVoronDirectory directory, Transaction readTransaction, IndexSearcher indexSearcher) : base(index, directory, readTransaction, indexSearcher)
    {
    }

    internal override void AddPopularity(SuggestWord suggestion, ref SuggestionResult result)
    {
        result = result.AddPopularity(suggestion);
    }
}
