using System.Linq;

using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Static
{
    public class StaticMapIndex : Index<StaticMapIndexDefinition>
    {
        public StaticMapIndex(int indexId, StaticMapIndexDefinition definition)
            : base(indexId, IndexType.Map, definition)
        {
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            throw new System.NotImplementedException();
        }

        public override void HandleDelete(
            DocumentTombstone tombstone,
            IndexWriteOperation writer,
            TransactionOperationContext indexContext,
            IndexingStatsScope stats)
        {
            throw new System.NotImplementedException();
        }

        public override void HandleMap(
            Document document,
            IndexWriteOperation writer,
            TransactionOperationContext indexContext,
            IndexingStatsScope stats)
        {
            throw new System.NotImplementedException();
        }

        public override IQueryResultRetriever GetQueryResultRetriever(
            DocumentsOperationContext documentsContext,
            TransactionOperationContext indexContext,
            FieldsToFetch fieldsToFetch)
        {
            throw new System.NotImplementedException();
        }

        public static Index CreateNew(int indexId, IndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var staticIndex = IndexCompilationCache.GetIndexInstance(definition);
            var staticMapIndexDefinition = new StaticMapIndexDefinition(definition, staticIndex.Maps.Keys.ToArray());
            var instance = new StaticMapIndex(indexId, staticMapIndexDefinition);
            instance.Initialize(documentDatabase);

            return instance;
        }
    }
}