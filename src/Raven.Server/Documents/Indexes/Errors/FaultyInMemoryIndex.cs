using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Faceted;
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Errors
{
    internal class FaultyInMemoryIndex : Index
    {
        private readonly Exception _e;

        public FaultyInMemoryIndex(Exception e, long etag, string name, IndexingConfiguration configuration)
            : base(etag, IndexType.Faulty, new FaultyIndexDefinition(name ?? $"Faulty/Indexes/{etag}", new HashSet<string> { "@FaultyIndexes" },
                   IndexLockMode.Unlock, IndexPriority.Normal, new IndexField[0]))
        {
            _e = e;
            State = IndexState.Error;
            Configuration = configuration;
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            throw new NotSupportedException($"Index with etag {Etag} is in-memory implementation of a faulty index", _e);
        }

        public override IIndexedDocumentsEnumerator GetMapEnumerator(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            throw new NotSupportedException($"Index with etag {Etag} is in-memory implementation of a faulty index", _e);
        }

        public override void HandleDelete(DocumentTombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            throw new NotSupportedException($"Index with etag {Etag} is in-memory implementation of a faulty index", _e);
        }

        public override int HandleMap(LazyStringValue key, IEnumerable mapResults, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            throw new NotSupportedException($"Index with etag {Etag} is in-memory implementation of a faulty index", _e);
        }

        public override IQueryResultRetriever GetQueryResultRetriever(DocumentsOperationContext documentsContext, FieldsToFetch fieldsToFetch)
        {
            throw new NotSupportedException($"Index with etag {Etag} is in-memory implementation of a faulty index", _e);
        }

        public override void Update(IndexDefinitionBase definition, IndexingConfiguration configuration)
        {
            throw new NotSupportedException($"{Type} index does not support updating it's definition and configuration.");
        }

        public override List<IndexingError> GetErrors()
        {
            return new List<IndexingError>
            {
                new IndexingError
                {
                    Error = _e?.ToString(),
                    Action = $"Index with etag {Etag} is in-memory implementation of a faulty index"
                }
            };
        }

        public override IndexProgress GetProgress(DocumentsOperationContext documentsContext)
        {
            return new IndexProgress
            {
                Name = Name,
                Etag = Etag,
                Type = Type
            };
        }

        public override IndexStats GetStats(bool calculateLag = false, bool calculateStaleness = false, DocumentsOperationContext documentsContext = null)
        {
            return new IndexStats
            {
                Name = Name,
                Etag = Etag,
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

        public override void SetPriority(IndexPriority priority)
        {
            throw new NotSupportedException($"Index with etag {Etag} is in-memory implementation of a faulty index", _e);
        }

        public override void SetState(IndexState state)
        {
            throw new NotSupportedException($"Index with etag {Etag} is in-memory implementation of a faulty index", _e);
        }

        public override void Enable()
        {
            throw new NotSupportedException($"Index with etag {Etag} is in-memory implementation of a faulty index", _e);
        }

        public override void Disable()
        {
            throw new NotSupportedException($"Index with etag {Etag} is in-memory implementation of a faulty index", _e);
        }

        public override void SetLock(IndexLockMode mode)
        {
            throw new NotSupportedException($"Index with etag {Etag} is in-memory implementation of a faulty index", _e);
        }

        public override Task StreamQuery(HttpResponse response, BlittableJsonTextWriter writer, IndexQueryServerSide query, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            throw new NotSupportedException($"Index with etag {Etag} is in-memory implementation of a faulty index", _e);
        }

        public override Task<DocumentQueryResult> Query(IndexQueryServerSide query, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            throw new NotSupportedException($"Index with etag {Etag} is in-memory implementation of a faulty index", _e);
        }

        public override Task<FacetedQueryResult> FacetedQuery(FacetQueryServerSide query, long facetSetupEtag, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            throw new NotSupportedException($"Index with etag {Etag} is in-memory implementation of a faulty index", _e);
        }

        public override TermsQueryResultServerSide GetTerms(string field, string fromValue, int pageSize, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            throw new NotSupportedException($"Index with etag {Etag} is in-memory implementation of a faulty index", _e);
        }

        public override MoreLikeThisQueryResultServerSide MoreLikeThisQuery(MoreLikeThisQueryServerSide query, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            throw new NotSupportedException($"Index with etag {Etag} is in-memory implementation of a faulty index", _e);
        }
    }
}