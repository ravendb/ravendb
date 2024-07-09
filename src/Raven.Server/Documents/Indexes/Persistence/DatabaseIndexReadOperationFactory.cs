using Corax.Mappings;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Queries;
using Raven.Server.Indexing;
using Sparrow.Logging;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence;

public sealed class DatabaseIndexReadOperationFactory : IIndexReadOperationFactory
{
    public LuceneIndexReadOperation CreateLuceneIndexReadOperation(Index index, LuceneVoronDirectory directory, 
        QueryBuilderFactories queryBuilderFactories, Transaction readTransaction, IndexQueryServerSide query)
    {
        return new LuceneIndexReadOperation(index, directory, queryBuilderFactories, readTransaction, query);
    }

    public CoraxIndexReadOperation CreateCoraxIndexReadOperation(Index index, Logger logger, Transaction readTransaction, QueryBuilderFactories queryBuilderFactories,
        IndexFieldsMapping fieldsMapping, IndexQueryServerSide query)
    {
        return new CoraxIndexReadOperation(index, logger, readTransaction, queryBuilderFactories, fieldsMapping, query);
    }

    public LuceneSuggestionIndexReader CreateLuceneSuggestionIndexReader(Index index, LuceneVoronDirectory directory, 
        Transaction readTransaction)
    {
        return new LuceneSuggestionIndexReader(index, directory, readTransaction);
    }
}
