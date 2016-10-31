using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Abstractions.Indexing;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Errors
{
    public class FaultyInMemoryIndex : Index
    {
        private readonly Exception _e;

        public FaultyInMemoryIndex(Exception e ,int indexId, string name)
            : base(indexId, IndexType.Faulty, new FaultyIndexDefinition(name ?? $"Faulty/Indexes/{indexId}", new[] { "@FaultyIndexes" },
                   IndexLockMode.Unlock, new IndexField[0]))
        {
            _e = e;
            Priority = IndexingPriority.Error;
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index", _e);
        }

        public override IIndexedDocumentsEnumerator GetMapEnumerator(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index", _e);
        }

        public override void HandleDelete(DocumentTombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index", _e);
        }

        public override int HandleMap(LazyStringValue key, IEnumerable mapResults, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index", _e);
        }

        public override IQueryResultRetriever GetQueryResultRetriever(DocumentsOperationContext documentsContext, TransactionOperationContext indexContext, FieldsToFetch fieldsToFetch)
        {
            throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index", _e);
        }

        public override List<IndexingError> GetErrors()
        {
            return new List<IndexingError>
            {
                new IndexingError
                {
                    Error = _e?.ToString(),
                    Action = $"Index with id {IndexId} is in-memory implementation of a faulty index"
                }
            };
        }

        public override int? ActualMaxNumberOfIndexOutputs
        {
            get
            {
                throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index", _e);
            }
        }

        public override int MaxNumberOfIndexOutputs
        {
            get
            {
                throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index", _e);
            }
        }

        public override bool IsStale(DocumentsOperationContext databaseContext)
        {
            return false;
        }

        public override IndexProgress GetProgress(DocumentsOperationContext documentsContext)
        {
            return new IndexProgress
            {
                Name = Name,
                Id = IndexId,
                Type = Type
            };
        }

        public override IndexStats GetStats(bool calculateLag = false, bool calculateStaleness = false, DocumentsOperationContext documentsContext = null)
        {
            return new IndexStats
            {
                Name = Name,
                Id = IndexId,
                Type = Type
            };
        }

        public override IndexRunningStatus Status => IndexRunningStatus.Disabled;

        public override void Start()
        {
            // no-op
        }

        public override void Stop()
        {
            // no-op
        }

        public override void SetPriority(IndexingPriority priority)
        {
            throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index", _e);
        }

        public override void SetLock(IndexLockMode mode)
        {
            throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index", _e);
        }

        public override Task StreamQuery(HttpResponse response, BlittableJsonTextWriter writer, IndexQueryServerSide query, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index", _e);
        }

        public override Task<DocumentQueryResult> Query(IndexQueryServerSide query, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index", _e);
        }

        public override Task<FacetedQueryResult> FacetedQuery(FacetQuery query, long facetSetupEtag, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index", _e);
        }

        public override TermsQueryResult GetTerms(string field, string fromValue, int pageSize, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index", _e);
        }

        public override MoreLikeThisQueryResultServerSide MoreLikeThisQuery(MoreLikeThisQueryServerSide query, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            throw new NotSupportedException($"Index with id {IndexId} is in-memory implementation of a faulty index", _e);
        }
    }
}