using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Documents.Includes.Sharding;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Results.Sharding;
using Raven.Server.Documents.Queries.Sharding;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Handlers.Processors.Counters;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Documents.Sharding.Operations.Queries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Queries;

public sealed class ShardedQueryProcessor : ShardedQueryProcessorBase<ShardedQueryResult>
{
    public ShardedQueryProcessor(
        TransactionOperationContext context,
        ShardedDatabaseRequestHandler requestHandler,
        IndexQueryServerSide query,
        long? existingResultEtag,
        bool metadataOnly,
        CancellationToken token)
        : base(context, requestHandler, query, existingResultEtag, metadataOnly, indexEntriesOnly: false, ignoreLimit: false, token)
    {
    }

    public override async Task<ShardedQueryResult> ExecuteShardedOperations(QueryTimingsScope scope)
    {
        using (var queryScope = scope?.For(nameof(QueryTimingsScope.Names.Query)))
        {
            var documentsComparer = GetComparer(Query);
            ShardedQueryOperation operation;
            ShardedReadResult<ShardedQueryResult> shardedReadResult;

            using (var executeScope = queryScope?.For(nameof(QueryTimingsScope.Names.Execute)))
            {
                var commands = GetOperationCommands(executeScope);

                operation = new ShardedQueryOperation(Query, IsProjectionFromMapReduceIndex, Context, RequestHandler, commands, documentsComparer, ExistingResultEtag?.ToString());
                int[] shards = GetShardNumbers(commands);
                shardedReadResult = await RequestHandler.ShardExecutor.ExecuteParallelForShardsAsync(shards, operation, Token);
            }

            if (shardedReadResult.StatusCode == (int)HttpStatusCode.NotModified)
            {
                return new ShardedQueryResult { NotModified = true };
            }

            var result = shardedReadResult.Result;

            await WaitForRaftIndexIfNeededAsync(result.AutoIndexCreationRaftIndex, queryScope);

            using (Query.Metadata.HasIncludeOrLoad ? queryScope?.For(nameof(QueryTimingsScope.Names.Includes)) : null)
            {
                if (operation.MissingDocumentIncludes is { Count: > 0 })
                {
                    await HandleMissingDocumentIncludesAsync(Context, RequestHandler.HttpContext.Request, RequestHandler.DatabaseContext,
                        operation.MissingDocumentIncludes, result, MetadataOnly, Token);
                }

                if (operation.MissingCounterIncludes is { Count: > 0 })
                {
                    await HandleMissingCounterIncludeAsync(Context, RequestHandler.HttpContext.Request, RequestHandler.DatabaseContext,
                        operation.MissingCounterIncludes, result, Token);
                }

                if (operation.MissingTimeSeriesIncludes is { Count: > 0 })
                {
                    await HandleMissingTimeSeriesIncludesAsync(Context, RequestHandler.HttpContext.Request, RequestHandler.DatabaseContext,
                        operation.MissingTimeSeriesIncludes, result, Token);
                }
            }

            // For map/reduce - we need to re-run the reduce portion of the index again on the results
            ReduceResults(ref result, queryScope);

            // For map-reduce indexes we need to filter the result after the reduce
            FilterAfterMapReduce(ref result, queryScope);

            // For map-reduce indexes we project the results after the reduce part 
            ProjectAfterMapReduce(ref result, queryScope);

            ApplyPaging(ref result, queryScope);

            // * For JS projections and load clauses, we don't support calling load() on a
            //   document that is not on the same shard
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "RavenDB-17889 Add a test for that");

            return result;
        }
    }

    public static async ValueTask HandleMissingCounterIncludeAsync(TransactionOperationContext context, HttpRequest request, ShardedDatabaseContext databaseContext, HashSet<string> missingCounterIncludes, ShardedQueryResult result, CancellationToken token)
    {
        var counterBatch = new CounterBatch();

        foreach (string docId in missingCounterIncludes)
        {
            var includes = result.GetCounterIncludes();

            var counterOperations = new List<CounterOperation>();

            if (includes.IncludedCounterNames.TryGetValue(docId, out var counterNames) && counterNames.Length > 0)
            {
                foreach (string counterName in counterNames)
                {
                    counterOperations.Add(new CounterOperation
                    {
                        CounterName = counterName,
                        Type = CounterOperationType.Get
                    });
                }
            }
            else
            {
                counterOperations.Add(new CounterOperation
                {
                    Type = CounterOperationType.GetAll
                });
            }

            counterBatch.Documents.Add(new DocumentCountersOperation
            {
                DocumentId = docId,
                Operations = counterOperations,
            });
        }

        var commandsPerShard = new Dictionary<int, CounterBatchOperation.CounterBatchCommand>();

        var shardsToPositions = ShardLocator.GetDocumentIdsByShards(context, databaseContext, missingCounterIncludes);
        foreach (var (shardNumber, idsByShard) in shardsToPositions)
        {
            var countersBatchForShard = new CounterBatch()
            {
                ReplyWithAllNodesValues = counterBatch.ReplyWithAllNodesValues,
                Documents = new()
            };

            foreach (var pos in idsByShard.Positions)
            {
                countersBatchForShard.Documents.Add(counterBatch.Documents[pos]);
            }

            commandsPerShard[shardNumber] = new CounterBatchOperation.CounterBatchCommand(databaseContext.ShardExecutor.Conventions, countersBatchForShard);
        }

        var counterIncludes = await databaseContext.ShardExecutor.ExecuteParallelForShardsAsync(shardsToPositions.Keys.ToArray(),
            new ShardedCounterBatchOperation(request, commandsPerShard), token);

        foreach (var counterInclude in counterIncludes.Counters)
        {
            if (counterInclude == null)
                continue;

            ((ShardedCounterIncludes)result.GetCounterIncludes()).AddMissingCounter(counterInclude.DocumentId, context.ReadObject(counterInclude.ToJson(), counterInclude.DocumentId));
        }
    }

    public static async ValueTask HandleMissingTimeSeriesIncludesAsync(TransactionOperationContext context, HttpRequest request, ShardedDatabaseContext databaseContext, Dictionary<string, List<TimeSeriesRange>> missingTimeSeriesIncludes, ShardedQueryResult result, CancellationToken token)
    {
        var commandsPerShard = new Dictionary<int, GetMultipleTimeSeriesRangesCommand>();

        var shardsToPositions = ShardLocator.GetDocumentIdsByShards(context, databaseContext, missingTimeSeriesIncludes.Keys);

        foreach (var (shardNumber, idsByShard) in shardsToPositions)
        {
            var rangesForShard = new Dictionary<string, List<TimeSeriesRange>>();

            foreach (var id in idsByShard.Ids)
            {
                rangesForShard[id] = missingTimeSeriesIncludes[id];
            }

            commandsPerShard[shardNumber] = new GetMultipleTimeSeriesRangesCommand(databaseContext.ShardExecutor.Conventions, rangesForShard);
        }

        var timeSeriesIncludes = await databaseContext.ShardExecutor.ExecuteParallelForShardsAsync(shardsToPositions.Keys.ToArray(),
            new ShardedTimeSeriesOperation(request, commandsPerShard), token);

        foreach (var tsInclude in timeSeriesIncludes.Results)
        {
            if (tsInclude == null)
                continue;

            ((ShardedTimeSeriesIncludes)result.GetTimeSeriesIncludes()).AddMissingTimeSeries(tsInclude.Id, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(tsInclude.Values, context));
        }

    }

    private void FilterAfterMapReduce(ref ShardedQueryResult result, QueryTimingsScope scope)
    {
        if (Query.Metadata.Query.Filter == null)
            return;

        if (IndexType.IsMapReduce() == false && IsAutoMapReduceQuery == false)
            return;

        using (scope?.For(nameof(QueryTimingsScope.Names.Filter)))
        {
            var currentResults = result.Results;
            result.Results = new List<BlittableJsonReaderObject>();

            using (var queryFilter = new ShardedQueryFilter(Query, result, scope, RequestHandler.DatabaseContext.Indexes.ScriptRunnerCache, Context))
            {
                foreach (var doc in currentResults)
                {
                    var filterResult = queryFilter.Apply(doc);
                    if (filterResult == FilterResult.Skipped)
                        continue;

                    if (filterResult == FilterResult.LimitReached)
                        break;

                    result.Results.Add(doc);

                    if (result.Results.Count == Query.Limit)
                        break;
                }
            }
        }
    }


    private void ProjectAfterMapReduce(ref ShardedQueryResult result, QueryTimingsScope scope)
    {
        if (IsProjectionFromMapReduceIndex == false)
            return;

        using (scope?.For(nameof(QueryTimingsScope.Names.Projection)))
        {
            var fieldsToFetch = new FieldsToFetch(Query, indexDefinition, IndexType);
            var retriever = new ShardedMapReduceResultRetriever(RequestHandler.DatabaseContext.Indexes.ScriptRunnerCache, Query, null, SearchEngineType.Lucene, fieldsToFetch, null, Context, null, null, null,
                RequestHandler.DatabaseContext.IdentityPartsSeparator);

            var currentResults = result.Results;
            result.Results = new List<BlittableJsonReaderObject>();

            foreach (var data in currentResults)
            {
                var retrieverInput = new RetrieverInput();
                (Document document, List<Document> documents) = retriever.GetProjectionFromDocument(new Document
                {
                    Data = data
                }, ref retrieverInput, fieldsToFetch, Context, CancellationToken.None);

                var results = result.Results;

                if (document != null)
                {
                    AddDocument(document.Data);
                }
                else if (documents != null)
                {
                    foreach (var doc in documents)
                    {
                        AddDocument(doc.Data);
                    }
                }

                void AddDocument(BlittableJsonReaderObject documentData)
                {
                    var doc = Context.ReadObject(documentData, "modified-map-reduce-result");
                    results.Add(doc);
                }
            }
        }
    }
}
