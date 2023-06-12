using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Sharding.Commands.Querying;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations.Queries;

public abstract class AbstractShardedQueryOperation<TCombinedResult, TResult, TIncludes> : IShardedReadOperation<QueryResult, TCombinedResult>
{
    private readonly ShardedDatabaseRequestHandler _requestHandler;

    protected readonly Dictionary<int, ShardedQueryCommand> QueryCommands;

    protected readonly TransactionOperationContext Context;
    protected readonly bool HadActiveMigrationsBeforeQueryStarted;
    protected long CombinedResultEtag;

    protected AbstractShardedQueryOperation(
        [NotNull] QueryMetadata metadata,
        [NotNull] Dictionary<int, ShardedQueryCommand> queryCommands,
        [NotNull] TransactionOperationContext context,
        [NotNull] ShardedDatabaseRequestHandler requestHandler,
        string expectedEtag)
    {
        if (metadata == null)
            throw new ArgumentNullException(nameof(metadata));

        QueryCommands = queryCommands ?? throw new ArgumentNullException(nameof(queryCommands));
        Context = context ?? throw new ArgumentNullException(nameof(context));
        _requestHandler = requestHandler ?? throw new ArgumentNullException(nameof(requestHandler));
        ExpectedEtag = metadata.HasOrderByRandom == false ? expectedEtag : null;
        
        HadActiveMigrationsBeforeQueryStarted = requestHandler.DatabaseContext.DatabaseRecord.Sharding.HasActiveMigrations();
    }

    public HttpRequest HttpRequest { get => _requestHandler.HttpContext.Request; }

    public string ExpectedEtag { get; }

    public HashSet<string> MissingDocumentIncludes { get; private set; }

    RavenCommand<QueryResult> IShardedOperation<QueryResult, ShardedReadResult<TCombinedResult>>.CreateCommandForShard(int shardNumber) => QueryCommands[shardNumber];

    public string CombineCommandsEtag(Dictionary<int, ShardExecutionResult<QueryResult>> commands)
    {
        CombinedResultEtag = 0;

        foreach (var cmd in commands.Values)
        {
            CombinedResultEtag = Hashing.Combine(CombinedResultEtag, cmd.Result.ResultEtag);
        }

        return CharExtensions.ToInvariantString(CombinedResultEtag);
    }

    public abstract TCombinedResult CombineResults(Dictionary<int, ShardExecutionResult<QueryResult>> results);

    protected static void CombineSingleShardResultProperties(QueryResult<List<TResult>, List<TIncludes>> combinedResult, QueryResult singleShardResult)
    {
        // unbounded collection query compatibility to non-sharded (single shard total results is -1 in such case, we set it as -1 only once)
        if (combinedResult.TotalResults != -1 || singleShardResult.TotalResults != -1)
            combinedResult.TotalResults += singleShardResult.TotalResults;
        combinedResult.IsStale |= singleShardResult.IsStale;

        combinedResult.SkippedResults = 0; // sharded queries start from 0 on all shards and we apply paging on the orchestrator side

        combinedResult.IndexName = singleShardResult.IndexName;
        combinedResult.IncludedPaths = singleShardResult.IncludedPaths;

        if (combinedResult.IndexTimestamp < singleShardResult.IndexTimestamp)
            combinedResult.IndexTimestamp = singleShardResult.IndexTimestamp;

        if (combinedResult.LastQueryTime < singleShardResult.LastQueryTime)
            combinedResult.LastQueryTime = singleShardResult.LastQueryTime;

        if (singleShardResult.ScannedResults != null)
        {
            combinedResult.ScannedResults ??= 0;
            combinedResult.ScannedResults += singleShardResult.ScannedResults;
        }

        if (singleShardResult.IndexDefinitionRaftIndex.HasValue)
        {
            if (combinedResult.IndexDefinitionRaftIndex != null && combinedResult.IndexDefinitionRaftIndex != singleShardResult.IndexDefinitionRaftIndex)
                combinedResult.IsStale = true;

            if (combinedResult.IndexDefinitionRaftIndex == null || singleShardResult.IndexDefinitionRaftIndex > combinedResult.IndexDefinitionRaftIndex)
                combinedResult.IndexDefinitionRaftIndex = singleShardResult.IndexDefinitionRaftIndex;
        }

        if (singleShardResult.AutoIndexCreationRaftIndex.HasValue)
        {
            if (combinedResult.AutoIndexCreationRaftIndex == null || singleShardResult.AutoIndexCreationRaftIndex > combinedResult.AutoIndexCreationRaftIndex)
                combinedResult.AutoIndexCreationRaftIndex = singleShardResult.AutoIndexCreationRaftIndex;
        }
    }

    protected void HandleDocumentIncludes(QueryResult cmdResult, QueryResult<List<TResult>, List<TIncludes>> result)
    {
        HashSet<string> missingDocumentIncludes = MissingDocumentIncludes;
        HandleDocumentIncludesInternal(cmdResult.Includes, Context, result, ref missingDocumentIncludes);
        MissingDocumentIncludes = missingDocumentIncludes;
    }

    public static void HandleDocumentIncludesInternal(BlittableJsonReaderObject includes, JsonOperationContext context, QueryResult<List<TResult>, List<TIncludes>> result, ref HashSet<string> missingDocumentIncludes)
    {
        foreach (var id in includes.GetPropertyNames())
        {
            if (includes.TryGet(id, out BlittableJsonReaderObject include) && include != null)
            {
                if (result.Includes is List<BlittableJsonReaderObject> blittableIncludes)
                    blittableIncludes.Add(include.Clone(context));
                else if (result.Includes is List<Document> documentIncludes)
                    documentIncludes.Add(new Document { Id = context.GetLazyString(id), Data = include.Clone(context) });
                else
                    throw new NotSupportedException($"Unknown includes type: {result.Includes.GetType().FullName}");
            }
            else
            {
                (missingDocumentIncludes ??= new HashSet<string>()).Add(id);
            }
        }
    }

    protected void CombineExplanations<TQueryResult, TQueryIncludes>(QueryResult<TQueryResult, TQueryIncludes> result, ShardExecutionResult<QueryResult> shardResult)
    {
        if (shardResult.Result.Explanations is not { Count: > 0 })
            return;

        result.Explanations ??= new Dictionary<string, string[]>();

        foreach (var kvp in shardResult.Result.Explanations)
            result.Explanations[kvp.Key] = kvp.Value;
    }

    protected void CombineTimings(int shardNumber, ShardExecutionResult<QueryResult> shardResult)
    {
        QueryCommands[shardNumber].Scope?.WithBase(shardResult.Result.Timings);
    }
}
