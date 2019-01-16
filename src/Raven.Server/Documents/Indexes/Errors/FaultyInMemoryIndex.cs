using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

using Sparrow.Json;
using FacetQuery = Raven.Server.Documents.Queries.Facets.FacetQuery;

namespace Raven.Server.Documents.Indexes.Errors
{
    internal class FaultyInMemoryIndex : Index
    {
        private readonly Exception _e;

        private readonly DateTime _createdAt;

        public FaultyInMemoryIndex(Exception e, string name, IndexingConfiguration configuration, AutoIndexDefinitionBase definition)
            : this(e, configuration, new FaultyAutoIndexDefinition(name, new HashSet<string> { "@FaultyIndexes" }, IndexLockMode.Unlock, IndexPriority.Normal, new IndexField[0], definition))
        {
        }

        public FaultyInMemoryIndex(Exception e, string name, IndexingConfiguration configuration, IndexDefinition definition)
            : this(e, configuration, new FaultyIndexDefinition(name, new HashSet<string> { "@FaultyIndexes" }, IndexLockMode.Unlock, IndexPriority.Normal, new IndexField[0], definition))
        {
        }

        private FaultyInMemoryIndex(Exception e, IndexingConfiguration configuration, IndexDefinitionBase definition)
            : base(IndexType.Faulty, definition)
        {
            _e = e;
            _createdAt = DateTime.UtcNow;
            State = IndexState.Error;
            Configuration = configuration;
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override IIndexedDocumentsEnumerator GetMapEnumerator(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext, IndexingStatsScope stats, IndexType type)
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override void HandleDelete(Tombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override int HandleMap(LazyStringValue lowerId, LazyStringValue id, IEnumerable mapResults, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override IQueryResultRetriever GetQueryResultRetriever(IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsOperationContext documentsContext, FieldsToFetch fieldsToFetch, IncludeDocumentsCommand includeDocumentsCommand)
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override void Update(IndexDefinitionBase definition, IndexingConfiguration configuration)
        {
            throw new NotSupportedException($"{Type} index does not support updating it's definition and configuration.");
        }

        public override void SaveLastState()
        {
            throw new NotSupportedException($"{Type} index does not support flushing it's filters.");
        }

        public override List<IndexingError> GetErrors()
        {
            return new List<IndexingError>
            {
                new IndexingError
                {
                    Error = _e?.ToString(),
                    Action = $"Index {Name} is in-memory implementation of a faulty index",
                    Timestamp = _createdAt
                }
            };
        }

        public override IndexProgress GetProgress(DocumentsOperationContext documentsContext, bool? isStale = null)
        {
            return new IndexProgress
            {
                Name = Name,
                Type = Type
            };
        }

        public override IndexStats GetStats(bool calculateLag = false, bool calculateStaleness = false, DocumentsOperationContext documentsContext = null)
        {
            return new IndexStats()
            {
                Name = Name,
                Type = Type
            };
        }

        public override IndexRunningStatus Status => IndexRunningStatus.Disabled;

        public override void Start()
        {
            // no-op
        }

        public override void Stop(bool disableIndex = false)
        {
            // no-op
        }

        public override void SetPriority(IndexPriority priority)
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override void SetState(IndexState state, bool inMemoryOnly = false, bool ignoreWriteError = false)
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override void Enable()
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override void Disable()
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override void SetLock(IndexLockMode mode)
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override Task StreamQuery(HttpResponse response, IStreamQueryResultWriter<Document> writer, IndexQueryServerSide query, QueryRunner.QueryMarker queryMarker, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override Task<DocumentQueryResult> Query(IndexQueryServerSide query, QueryRunner.QueryMarker queryMarker, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override Task<FacetedQueryResult> FacetedQuery(FacetQuery query, QueryRunner.QueryMarker queryMarker, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override TermsQueryResultServerSide GetTerms(string field, string fromValue, int pageSize, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override (ICollection<string> Static, ICollection<string> Dynamic) GetEntriesFields()
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }
    }
}
