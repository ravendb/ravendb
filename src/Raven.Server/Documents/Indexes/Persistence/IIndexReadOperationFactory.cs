using Corax.Mappings;
using Lucene.Net.Search;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Queries;
using Raven.Server.Indexing;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence;

public interface IIndexReadOperationFactory
{
    public LuceneIndexReadOperation CreateLuceneIndexReadOperation(Index index, LuceneVoronDirectory directory, 
        QueryBuilderFactories queryBuilderFactories, Transaction readTransaction, IndexQueryServerSide query);

    public CoraxIndexReadOperation CreateCoraxIndexReadOperation(Index index, RavenLogger logger, Transaction readTransaction, QueryBuilderFactories queryBuilderFactories,
        IndexFieldsMapping fieldsMapping, IndexQueryServerSide query);

    public LuceneSuggestionIndexReader CreateLuceneSuggestionIndexReader(Index index, LuceneVoronDirectory directory,
        Transaction readTransaction, IndexSearcher indexSearcher);

}
