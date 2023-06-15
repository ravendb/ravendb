using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Explanation;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Documents.Indexes.Persistence
{
    public abstract class IndexReadOperationBase : IndexOperationBase
    {
        protected readonly QueryBuilderFactories QueryBuilderFactories;
        private readonly MemoryInfo _memoryInfo;

        protected IndexReadOperationBase(Index index, Logger logger, QueryBuilderFactories queryBuilderFactories, IndexQueryServerSide query) : base(index, logger)
        {
            QueryBuilderFactories = queryBuilderFactories;

            if (_logger.IsInfoEnabled && query != null)
            {
                _memoryInfo = new MemoryInfo
                {
                    AllocatedManagedBefore = GC.GetAllocatedBytesForCurrentThread(),
                    AllocatedUnmanagedBefore = NativeMemory.ThreadAllocations.Value.TotalAllocated,
                    ManagedThreadId = NativeMemory.CurrentThreadStats.ManagedThreadId,
                    Query = query.Metadata.Query
                };
            }
        }

        public abstract long EntriesCount();

        internal virtual void AssertCanOrderByScoreAutomaticallyWhenBoostingIsInvolved()
        {
        }
        
        public abstract IEnumerable<QueryResult> Query(IndexQueryServerSide query, QueryTimingsScope queryTimings, FieldsToFetch fieldsToFetch,
            Reference<long> totalResults, Reference<long> skippedResults, Reference<long> scannedDocuments, IQueryResultRetriever retriever, DocumentsOperationContext documentsContext,
            Func<string, SpatialField> getSpatialField, CancellationToken token);

        public abstract IEnumerable<QueryResult> IntersectQuery(IndexQueryServerSide query, FieldsToFetch fieldsToFetch, Reference<long> totalResults,
            Reference<long> skippedResults, Reference<long> scannedDocuments, IQueryResultRetriever retriever, DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField,
            CancellationToken token);

        public abstract SortedSet<string> Terms(string field, string fromValue, long pageSize, CancellationToken token);

        public abstract IEnumerable<QueryResult> MoreLikeThis(
            IndexQueryServerSide query,
            IQueryResultRetriever retriever,
            DocumentsOperationContext context,
            CancellationToken token);

        public abstract IEnumerable<BlittableJsonReaderObject> IndexEntries(IndexQueryServerSide query, Reference<long> totalResults, DocumentsOperationContext documentsContext,
            Func<string, SpatialField> getSpatialField, bool ignoreLimit, CancellationToken token);

        public abstract IEnumerable<string> DynamicEntriesFields(HashSet<string> staticFields);

        public override void Dispose()
        {
            if (_logger.IsInfoEnabled && _memoryInfo != null && _memoryInfo.ManagedThreadId == NativeMemory.CurrentThreadStats.ManagedThreadId)
            {
                var mangedDiff = GC.GetAllocatedBytesForCurrentThread() - _memoryInfo.AllocatedManagedBefore;
                var unmanagedDiff = Math.Max(0, NativeMemory.ThreadAllocations.Value.TotalAllocated - _memoryInfo.AllocatedUnmanagedBefore);

                if (mangedDiff > 0 || unmanagedDiff > 0)
                {
                    var msg = $"Query for index `{_indexName}` for query: `{_memoryInfo.Query}`, " +
                              $"allocated managed: {new Size(mangedDiff, SizeUnit.Bytes)}, " +
                              $"allocated unmanaged: {new Size(unmanagedDiff, SizeUnit.Bytes)}, " +
                              $"managed thread id: {_memoryInfo.ManagedThreadId}";

                    _logger.Info(msg);
                }
            }
        }
        
        public struct QueryResult
        {
            public Document Result;
            public Dictionary<string, Dictionary<string, string[]>> Highlightings;
            public ExplanationResult Explanation;
        }

        private class MemoryInfo
        {
            public long AllocatedManagedBefore { get; init; }
            public long AllocatedUnmanagedBefore { get; init; }
            public int ManagedThreadId { get; init; }
            public Queries.AST.Query Query { get; init; }
        }
    }
}
