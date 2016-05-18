using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes
{
    public abstract class MapIndexBase<T> : Index<T> where T : IndexDefinitionBase
    {
        protected MapIndexBase(int indexId, IndexType type, T definition) : base(indexId, type, definition)
        {
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            return new IIndexingWork[]
            {
                new CleanupDeletedDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, DocumentDatabase.Configuration.Indexing, null),
                new MapDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, DocumentDatabase.Configuration.Indexing, null),
            };
        }

        public override void HandleDelete(DocumentTombstone tombstone, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            writer.Delete(tombstone.Key, stats);
        }

        public override void HandleMap(Document document, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            writer.IndexDocument(document, stats);

            DocumentDatabase.Metrics.IndexedPerSecond.Mark();
        }

        public override IQueryResultRetriever GetQueryResultRetriever(DocumentsOperationContext documentsContext, TransactionOperationContext indexContext, FieldsToFetch fieldsToFetch)
        {
            return new MapQueryResultRetriever(DocumentDatabase.DocumentsStorage, documentsContext, fieldsToFetch);
        }
    }
}