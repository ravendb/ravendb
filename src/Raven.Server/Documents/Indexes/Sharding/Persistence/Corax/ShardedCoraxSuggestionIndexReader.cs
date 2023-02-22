using Corax.Mappings;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Sharding.Queries;
using Sparrow.Logging;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Sharding.Persistence.Corax;

public sealed class ShardedCoraxSuggestionIndexReader : CoraxSuggestionReader
{
    public ShardedCoraxSuggestionIndexReader(Index index, Logger logger, IndexFieldBinding binding, Transaction readTransaction, IndexFieldsMapping fieldsMapping) : base(index, logger, binding, readTransaction, fieldsMapping)
    {
    }

    internal override void AddPopularity(SuggestWord suggestion, ref SuggestionResult result)
    {
        result = result.AddPopularity(suggestion);
    }
}
