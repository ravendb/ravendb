using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using FacetQuery = Raven.Server.Documents.Queries.Facets.FacetQuery;

namespace Raven.Server.Documents.Indexes.Errors
{
    internal sealed class FaultyInMemoryIndex : Index
    {
        private readonly Exception _e;

        private readonly DateTime _createdAt;

        public FaultyInMemoryIndex(Exception e, string name, IndexingConfiguration configuration, AutoIndexDefinitionBaseServerSide definition, SearchEngineType searchEngineType)
            : this(e, configuration, new FaultyAutoIndexDefinition(name, new HashSet<string> { "@FaultyIndexes" }, IndexLockMode.Unlock, IndexPriority.Normal, IndexState.Normal, new IndexField[0], definition), searchEngineType)
        {
        }

        public FaultyInMemoryIndex(Exception e, string name, IndexingConfiguration configuration, IndexDefinition definition, SearchEngineType searchEngineType)
            : this(e, configuration, new FaultyIndexDefinition(name, new HashSet<string> { "@FaultyIndexes" }, IndexLockMode.Unlock, IndexPriority.Normal, IndexState.Normal, new IndexField[0], definition), searchEngineType)
        {
            if (searchEngineType is SearchEngineType.None && definition.Configuration.TryGetValue(RavenConfiguration.GetKey(i => i.Indexing.StaticIndexingEngineType), out var configKey))
            {
                if (Enum.TryParse(configKey, out Client.Documents.Indexes.SearchEngineType searchEngineKey))
                    SearchEngineType = searchEngineKey;
            }
        }

        private FaultyInMemoryIndex(Exception e, IndexingConfiguration configuration, IndexDefinitionBaseServerSide definition, SearchEngineType searchEngineType)
            : base(IndexType.Faulty, IndexSourceType.None, definition)
        {
            _e = e;
            _createdAt = DateTime.UtcNow;
            State = IndexState.Error;
            Configuration = configuration;
            SearchEngineType = searchEngineType;
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override IIndexedItemEnumerator GetMapEnumerator(IEnumerable<IndexItem> items, string collection, TransactionOperationContext indexContext, IndexingStatsScope stats, IndexType type)
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override void HandleDelete(Tombstone tombstone, string collection, Lazy<IndexWriteOperationBase> writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override int HandleMap(IndexItem indexItem, IEnumerable mapResults, Lazy<IndexWriteOperationBase> writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override IQueryResultRetriever GetQueryResultRetriever(IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsOperationContext documentsContext, SearchEngineType searchEngineType, FieldsToFetch fieldsToFetch, IncludeDocumentsCommand includeDocumentsCommand, IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand, IncludeRevisionsCommand includeRevisionsCommand)
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override void Update(IndexDefinitionBaseServerSide definition, IndexingConfiguration configuration)
        {
            throw new NotSupportedException($"{Type} index does not support updating it's definition and configuration.");
        }

        public override void SaveLastState()
        {
            throw new NotSupportedException($"{Type} index does not support flushing it's filters.");
        }

        public override void DeleteErrors()
        {
            // no-op
        }

        public override List<IndexingError> GetErrors()
        {
            return new List<IndexingError>
            {
                new IndexingError
                {
                    Error = _e?.ToString(),
                    Action = "Faulty",
                    Timestamp = _createdAt
                }
            };
        }

        internal override IndexProgress GetProgress(QueryOperationContext queryContext, Stopwatch overallDuration, bool? isStale = null)
        {
            return new IndexProgress
            {
                Name = Name,
                Type = Type
            };
        }

        public override IndexStats GetStats(bool calculateLag = false, bool calculateStaleness = false, bool calculateMemoryStats = false, bool calculateLastBatchStats = false, QueryOperationContext queryContext = null)
        {
            return new IndexStats
            {
                Name = Name,
                Type = Type,
                SearchEngineType = SearchEngineType
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

        public override Task StreamQuery(HttpResponse response, IStreamQueryResultWriter<Document> writer, IndexQueryServerSide query, QueryOperationContext queryContext, OperationCancelToken token)
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override Task<DocumentQueryResult> Query(IndexQueryServerSide query, QueryOperationContext queryContext, OperationCancelToken token)
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override Task<FacetedQueryResult> FacetedQuery(FacetQuery query, QueryOperationContext queryContext, OperationCancelToken token)
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override TermsQueryResultServerSide GetTerms(string field, string fromValue, long pageSize, QueryOperationContext queryContext, OperationCancelToken token)
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }

        public override (ICollection<string> Static, ICollection<string> Dynamic) GetEntriesFields()
        {
            throw new NotSupportedException($"Index {Name} is in-memory implementation of a faulty index", _e);
        }
    }
}
