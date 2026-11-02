using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Server.Documents.Indexes.Debugging;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Explanation;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server.Logging;
using Sparrow.Utils;

namespace Raven.Server.Documents.Indexes.Persistence
{
    public abstract class IndexReadOperationBase : IndexOperationBase
    {
        public abstract bool IsSharded { get; }
        protected readonly QueryBuilderFactories QueryBuilderFactories;
        private readonly QueryInfo _queryInfo;
        private static readonly long ThresholdForMemoryUsageLoggingInBytes = new Size(512, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes);

        protected IndexReadOperationBase(Index index, RavenLogger logger, QueryBuilderFactories queryBuilderFactories, IndexQueryServerSide query) : base(index, logger)
        {
            QueryBuilderFactories = queryBuilderFactories;

            if (query != null && logger.IsInfoEnabled)
            {
                _queryInfo = new QueryInfo
                {
                    AllocatedManagedBefore = GC.GetAllocatedBytesForCurrentThread(),
                    AllocatedUnmanagedBefore = NativeMemory.ThreadAllocations.Value.TotalAllocated,
                    ManagedThreadId = NativeMemory.CurrentThreadStats.ManagedThreadId,
                    Query = query.Metadata.Query,
                    PageSize = query.PageSize
                };
            }
        }

        public abstract long EntriesCount();

        public void TrackResultsForLogging(Reference<long> totalResults, Reference<long> skippedResults)
        {
            if (_queryInfo == null)
                return;

            _queryInfo.TotalResults = totalResults;
            _queryInfo.SkippedResults = skippedResults;
        }

        internal virtual void AssertCanOrderByScoreAutomaticallyWhenBoostingOrVectorSearchIsInvolved(bool hasVectorSearch)
        {
        }

        public abstract IEnumerable<QueryResult> Query(IndexQueryServerSide query, QueryTimingsScope queryTimings, FieldsToFetch fieldsToFetch,
            Reference<long> totalResults, Reference<long> skippedResults, Reference<long> scannedDocuments, IQueryResultRetriever retriever, DocumentsOperationContext documentsContext,
            Func<string, SpatialField> getSpatialField, QueryTimeScope queryTime, CancellationToken token);

        public abstract IEnumerable<QueryResult> IntersectQuery(IndexQueryServerSide query, FieldsToFetch fieldsToFetch, Reference<long> totalResults,
            Reference<long> skippedResults, Reference<long> scannedDocuments, IQueryResultRetriever retriever, DocumentsOperationContext documentsContext, Func<string, SpatialField> getSpatialField,
            QueryTimeScope queryTime, CancellationToken token);

        public abstract List<string> Terms(string field, string fromValue, long pageSize, CancellationToken token);

        public abstract IEnumerable<QueryResult> MoreLikeThis(
            IndexQueryServerSide query,
            IQueryResultRetriever retriever,
            DocumentsOperationContext context,
            CancellationToken token);

        public abstract IEnumerable<BlittableJsonReaderObject> IndexEntries(IndexQueryServerSide query, Reference<long> totalResults, DocumentsOperationContext documentsContext,
            Func<string, SpatialField> getSpatialField, bool ignoreLimit, CancellationToken token);

        public abstract HashSet<FieldDebugInfo> GetEntriesFields(ICollection<string> unknownTypeStaticFields);

        protected static bool IsDynamicFieldKnownAsStatic(string fieldName)
        {
            return fieldName
                is Client.Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName 
                or Client.Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName
                or Client.Constants.Documents.Indexing.Fields.ValueFieldName
                or Client.Constants.Documents.Indexing.Fields.DocumentIdFieldName
                or Client.Constants.Documents.Indexing.Fields.SourceDocumentIdFieldName;
        }

        public override void Dispose()
        {
            if ((_logger.IsInfoEnabled || (_logger.IsWarnEnabled && _index.IsLowMemory)) &&
                _queryInfo != null && _queryInfo.ManagedThreadId == NativeMemory.CurrentThreadStats.ManagedThreadId)
            {
                // information logs are enabled or we're in a low memory state and operations logs are enabled

                var mangedDiff = GC.GetAllocatedBytesForCurrentThread() - _queryInfo.AllocatedManagedBefore;
                var unmanagedDiff = Math.Max(0, NativeMemory.ThreadAllocations.Value.TotalAllocated - _queryInfo.AllocatedUnmanagedBefore);

                if (_logger.IsWarnEnabled && mangedDiff < ThresholdForMemoryUsageLoggingInBytes && unmanagedDiff < ThresholdForMemoryUsageLoggingInBytes)
                {
                    // we don't want to spam the logs when we are in a low memory state and operations logs are enabled
                    return;
                }

                var msg = $"Query for index `{_indexName}` for query: `{_queryInfo.Query}`, " +
                          $"page size: {_queryInfo.PageSize:#,#;;0}, " +
                          $"total results: {FormatCount(_queryInfo.TotalResults)}, " +
                          $"skipped results: {FormatCount(_queryInfo.SkippedResults)}, " +
                          $"allocated managed: {new Size(mangedDiff, SizeUnit.Bytes)}, " +
                          $"allocated unmanaged: {new Size(unmanagedDiff, SizeUnit.Bytes)}, " +
                          $"managed thread id: {_queryInfo.ManagedThreadId}";

                if (_logger.IsWarnEnabled)
                {
                    _logger.Warn(msg);
                }
                else
                {
                    _logger.Info(msg);
                }
            }
        }

        private static string FormatCount(Reference<long> count)
        {
            return count == null ? "unknown" : count.Value.ToString("#,#;;0");
        }

        public struct QueryResult
        {
            public Document Result;
            public Dictionary<string, Dictionary<string, string[]>> Highlightings;
            public ExplanationResult Explanation;
        }

        private sealed class QueryInfo
        {
            public long AllocatedManagedBefore { get; init; }
            public long AllocatedUnmanagedBefore { get; init; }
            public int ManagedThreadId { get; init; }
            public Queries.AST.Query Query { get; init; }
            public long PageSize { get; init; }
            public Reference<long> TotalResults { get; set; }
            public Reference<long> SkippedResults { get; set; }
        }
    }
}
