using System;
using System.Threading;
using NetTopologySuite.Algorithm;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Documents.Sharding.Commands.Querying;
using Raven.Server.Documents.Sharding.Comparers;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Queries;

/// <summary>
/// A struct that we use to hold state and break the process
/// of handling a sharded query into distinct steps
/// </summary>
public abstract class ShardedQueryProcessorBase<TCombinedResult> : AbstractShardedQueryProcessor<ShardedQueryCommand, QueryResult, TCombinedResult>
    where TCombinedResult : QueryResultServerSide<BlittableJsonReaderObject>
{
    protected ShardedQueryProcessorBase(
        TransactionOperationContext context,
        ShardedDatabaseRequestHandler requestHandler,
        IndexQueryServerSide query,
        long? existingResultEtag,
        bool metadataOnly,
        bool indexEntriesOnly,
        bool ignoreLimit,
        CancellationToken token)
        : base(context, requestHandler, query, metadataOnly, indexEntriesOnly, ignoreLimit, existingResultEtag, token)
    {

    }

    protected override ShardedQueryCommand CreateCommand(int shardNumber, BlittableJsonReaderObject query, QueryTimingsScope scope) => CreateShardedQueryCommand(shardNumber, query, scope);

    protected void ApplyPaging(ref TCombinedResult result, QueryTimingsScope scope)
    {
        QueryTimingsScope pagingScope = null;
        
        var queryOffset = Query.Offset is null && Query.Limit is null ? Query.Start : Query.Offset ?? 0;
        var queryLimit = Query.Offset is null && Query.Limit is null ? Query.PageSize : Query.Limit ?? long.MaxValue;
        
        
        
        if (queryOffset > 0)
        {
            using (GetPagingScope())
            {
                var count = Math.Min(result.Results.Count, Math.Min(queryOffset, int.MaxValue));
                result.Results.RemoveRange(0, (int)count);
            }
        }

        var limit = Math.Min(queryLimit, Query.FilterLimit ?? long.MaxValue);
        if (result.Results.Count > limit)
        {
            using (GetPagingScope())
            {
                long index = Math.Min(limit, int.MaxValue);
                long count = result.Results.Count - limit;
                if (count > int.MaxValue)
                    count = int.MaxValue;
                result.Results.RemoveRange((int)index, (int)count);
            }
        }

        QueryTimingsScope GetPagingScope()
        {
            if (scope == null)
                return null;

            pagingScope ??= scope.For(nameof(QueryTimingsScope.Names.Paging), start: false);
            pagingScope.Start();

            return pagingScope;
        }
    }

    protected void ReduceResults(ref TCombinedResult result, QueryTimingsScope scope)
    {
        if (IndexType.IsMapReduce() == false && IsAutoMapReduceQuery == false)
            return;

        using (scope?.For(nameof(QueryTimingsScope.Names.Reduce)))
        {
            var merger = CreateMapReduceQueryResultsMerger(result);
            result.Results = merger.Merge();

            var orderByFields = Query.Metadata.CachedOrderBy ?? Query.Metadata.OrderBy;
            if (orderByFields?.Length > 0)
            {
                // apply ordering after the re-reduce of a map-reduce index
                result.Results.Sort(new DocumentsComparer(orderByFields, extractFromData: true, Query.Metadata.HasOrderByRandom));
            }
        }
    }

    protected virtual ShardedMapReduceQueryResultsMerger CreateMapReduceQueryResultsMerger(TCombinedResult result) => new(result.Results, RequestHandler.DatabaseContext.Indexes, result.IndexName, IsAutoMapReduceQuery, Context, Token);
}
