using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Abstractions.Indexing;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.ServerWide.Context;

using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Errors
{
    public class FaultyInMemoryIndex : Index
    {
        public FaultyInMemoryIndex(int indexId, string name)
            : base(indexId, IndexType.Faulty, new FaultyIndexDefinition(name ?? $"Faulty/Indexes/{indexId}", new[] { "@FaultyIndexes" },
                   IndexLockMode.Unlock, new IndexField[0]))
        {
            Priority = IndexingPriority.Error;
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index");
        }

        public override IIndexedDocumentsEnumerator GetMapEnumerator(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext)
        {
            throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index");
        }

        public override void HandleDelete(DocumentTombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index");
        }

        public override void HandleMap(LazyStringValue key, IEnumerable mapResults, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index");
        }

        public override IQueryResultRetriever GetQueryResultRetriever(DocumentsOperationContext documentsContext, TransactionOperationContext indexContext, FieldsToFetch fieldsToFetch)
        {
            throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index");
        }

        public override int? ActualMaxNumberOfIndexOutputs
        {
            get
            {
                throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index");
            }
        }

        public override int MaxNumberOfIndexOutputs
        {
            get
            {
                throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index");
            }
        }
    }
}