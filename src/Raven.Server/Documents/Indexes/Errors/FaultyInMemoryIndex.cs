using System;
using Raven.Abstractions.Indexing;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.Errors
{
    public class FaultyInMemoryIndex : Index
    {
        public FaultyInMemoryIndex(int indexId, string name)
            : base(indexId, IndexType.Unknown, new FaultyIndexDefinition(name ?? $"Faulty/Indexes/{indexId}", new[] { "@FaultyIndexes" }, 
                   IndexLockMode.Unlock, new IndexField[0]))
        {
            Priority = IndexingPriority.Error;
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index");
        }

        public override void HandleDelete(DocumentTombstone tombstone, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index");
        }

        public override void HandleMap(Document document, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index");
        }

        public override IQueryResultRetriever GetQueryResultRetriever(DocumentsOperationContext documentsContext,
            TransactionOperationContext indexContext)
        {
            throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index");
        }
    }
}