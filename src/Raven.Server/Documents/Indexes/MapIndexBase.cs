using System.Collections;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.ServerWide.Context;

using Sparrow.Json;

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

        public override void HandleDelete(DocumentTombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            writer.Delete(tombstone.Key, stats);
        }

        public override void HandleMap(LazyStringValue key, IEnumerable mapResults, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            writer.Delete(key, stats);

            foreach (var mapResult in mapResults)
            {
                writer.IndexDocument(key, mapResult, stats);
            }
            
            DocumentDatabase.Metrics.IndexedPerSecond.Mark();
        }

        public override IQueryResultRetriever GetQueryResultRetriever(DocumentsOperationContext documentsContext, TransactionOperationContext indexContext, FieldsToFetch fieldsToFetch)
        {
            return new MapQueryResultRetriever(DocumentDatabase.DocumentsStorage, documentsContext, fieldsToFetch);
        }
    }
}