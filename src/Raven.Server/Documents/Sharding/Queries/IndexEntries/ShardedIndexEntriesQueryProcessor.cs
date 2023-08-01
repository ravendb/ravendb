using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Sharding;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Documents.Sharding.Commands.Querying;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Operations.Queries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Queries.IndexEntries;

public sealed class ShardedIndexEntriesQueryProcessor : ShardedQueryProcessorBase<ShardedIndexEntriesQueryResult>
{
    public ShardedIndexEntriesQueryProcessor(
        TransactionOperationContext context,
        ShardedDatabaseRequestHandler requestHandler
        , IndexQueryServerSide query,
        long? existingResultEtag,
        bool ignoreLimit,
        CancellationToken token)
        : base(context, requestHandler, query, existingResultEtag, metadataOnly: false, indexEntriesOnly: true, ignoreLimit, token)
    {
    }

    public override async Task<ShardedIndexEntriesQueryResult> ExecuteShardedOperations(QueryTimingsScope scope)
    {
        var documentsComparer = GetComparer(Query);

        var commands = GetOperationCommands(scope: null);

        var operation = new ShardedIndexEntriesQueryOperation(Query, Context, RequestHandler, commands, documentsComparer, ExistingResultEtag?.ToString());
        int[] shards = GetShardNumbers(commands);
        var shardedReadResult = await RequestHandler.ShardExecutor.ExecuteParallelForShardsAsync(shards, operation, Token);

        if (shardedReadResult.StatusCode == (int)HttpStatusCode.NotModified)
        {
            return new ShardedIndexEntriesQueryResult { NotModified = true };
        }

        var result = shardedReadResult.Result;

        await WaitForRaftIndexIfNeededAsync(result.AutoIndexCreationRaftIndex, scope: null);

        // For map/reduce - we need to re-run the reduce portion of the index again on the results
        ReduceResults(ref result, scope: null);

        ApplyPaging(ref result, scope: null);

        return result;
    }

    protected override ShardedMapReduceQueryResultsMerger CreateMapReduceQueryResultsMerger(ShardedIndexEntriesQueryResult result) => new ShardedMapReduceIndexEntriesQueryResultsMerger(result.Results, RequestHandler.DatabaseContext.Indexes, result.IndexName, IsAutoMapReduceQuery, Context, Token);

    protected override ShardedQueryCommand CreateCommand(int shardNumber, BlittableJsonReaderObject query, QueryTimingsScope scope) => CreateShardedQueryCommand(shardNumber, query, scope);

    protected override QueryType GetQueryType() => QueryType.IndexEntries;
}
