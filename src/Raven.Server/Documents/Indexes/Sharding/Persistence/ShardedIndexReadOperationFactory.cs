using Corax.Mappings;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Sharding.Persistence.Corax;
using Raven.Server.Documents.Indexes.Sharding.Persistence.Lucene;
using Raven.Server.Documents.Queries;
using Raven.Server.Indexing;
using Sparrow.Logging;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Sharding.Persistence;

public sealed class ShardedIndexReadOperationFactory : IIndexReadOperationFactory
{
    public LuceneIndexReadOperation CreateLuceneIndexReadOperation(Index index, LuceneVoronDirectory directory, 
        QueryBuilderFactories queryBuilderFactories, Transaction readTransaction, IndexQueryServerSide query)
    {
        return new ShardedLuceneIndexReadOperation(index, directory,  queryBuilderFactories, readTransaction, query);
    }

    public CoraxIndexReadOperation CreateCoraxIndexReadOperation(Index index, Logger logger, Transaction readTransaction, QueryBuilderFactories queryBuilderFactories,
        IndexFieldsMapping fieldsMapping, IndexQueryServerSide query)
    {
        return new ShardedCoraxIndexReadOperation(index, logger, readTransaction, queryBuilderFactories, fieldsMapping, query);
    }

    public LuceneSuggestionIndexReader CreateLuceneSuggestionIndexReader(Index index, LuceneVoronDirectory directory, 
        Transaction readTransaction)
    {
        return new ShardedLuceneSuggestionIndexReader(index, directory,  readTransaction);
    }
}
