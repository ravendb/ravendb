using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Indexes.Auto
{
    public class AutoMapIndex : Index<AutoMapIndexDefinition>
    {
        private AutoMapIndex(int indexId, AutoMapIndexDefinition definition)
            : base(indexId, IndexType.AutoMap, definition)
        {
        }

        public static AutoMapIndex CreateNew(int indexId, AutoMapIndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var instance = new AutoMapIndex(indexId, definition);
            instance.Initialize(documentDatabase);

            return instance;
        }

        public static AutoMapIndex Open(int indexId, StorageEnvironment environment, DocumentDatabase documentDatabase)
        {
            var definition = AutoMapIndexDefinition.Load(environment);
            var instance = new AutoMapIndex(indexId, definition);
            instance.Initialize(environment, documentDatabase);

            return instance;
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            return new IIndexingWork[]
            {
                new CleanupDeletedDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, DocumentDatabase.Configuration.Indexing),
                new MapDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, DocumentDatabase.Configuration.Indexing), 
            };
        }

        public override void HandleDelete(DocumentTombstone tombstone, IndexWriteOperation writer, TransactionOperationContext indexContext)
        {
            writer.Delete(tombstone.Key);
        }

        public override void HandleMap(Document document, IndexWriteOperation writer, TransactionOperationContext indexContext)
        {
            writer.IndexDocument(document);

            DocumentDatabase.Metrics.IndexedPerSecond.Mark();
        }

        public override IQueryResultRetriever GetQueryResultRetriever(DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            return new DocumentQueryResultRetriever(DocumentDatabase.DocumentsStorage, documentsContext);
        }
    }
}