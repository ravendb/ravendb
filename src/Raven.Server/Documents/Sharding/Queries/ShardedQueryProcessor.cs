using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Documents.Includes.Sharding;
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

public class ShardedQueryProcessor : ShardedQueryProcessorBase<ShardedQueryResult>
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
            ShardedDocumentsComparer documentsComparer = null;

            if (Query.Metadata.OrderBy?.Length > 0 && (IsMapReduceIndex || IsAutoMapReduceQuery) == false && (Query.Limit is null || Query.Limit > 0))
            {
                // sorting only if:
                // 1. we have fields to sort
                // 2. it isn't a map-reduce index/query (the sorting will be done after the re-reduce)
                // 3. this isn't Count() query - Limit is 0 then
                documentsComparer = new ShardedDocumentsComparer(Query.Metadata, IsMapReduceIndex || IsAutoMapReduceQuery);
            }

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
                    await HandleMissingDocumentIncludes(operation.MissingDocumentIncludes, result);
                }

                if (operation.MissingCounterIncludes is { Count: > 0 })
                {
                    await HandleMissingCounterInclude(operation.MissingCounterIncludes, result);
                }

                if (operation.MissingTimeSeriesIncludes is { Count: > 0 })
                {
                    await HandleMissingTimeSeriesIncludes(operation.MissingTimeSeriesIncludes, result);
                }
            }

            // For map/reduce - we need to re-run the reduce portion of the index again on the results
            ReduceResults(ref result, queryScope);

            ApplyPaging(ref result, queryScope);

            // For map-reduce indexes we project the results after the reduce part 
            ProjectAfterMapReduce(ref result, queryScope);

            // * For JS projections and load clauses, we don't support calling load() on a
            //   document that is not on the same shard
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "RavenDB-17889 Add a test for that");

            return result;
        }
    }

    private async Task HandleMissingCounterInclude(HashSet<string> missingCounterIncludes, ShardedQueryResult result)
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

        var shardsToPositions = ShardLocator.GetDocumentIdsByShards(Context, RequestHandler.DatabaseContext, missingCounterIncludes);
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

            commandsPerShard[shardNumber] = new CounterBatchOperation.CounterBatchCommand(countersBatchForShard);
        }

        var counterIncludes = await RequestHandler.ShardExecutor.ExecuteParallelForShardsAsync(shardsToPositions.Keys.ToArray(),
            new ShardedCounterBatchOperation(RequestHandler.HttpContext, commandsPerShard), Token);

        foreach (var counterInclude in counterIncludes.Counters)
        {
            if (counterInclude == null)
                continue;

            ((ShardedCounterIncludes)result.GetCounterIncludes()).AddMissingCounter(counterInclude.DocumentId, Context.ReadObject(counterInclude.ToJson(), counterInclude.DocumentId));
        }
    }
    private async Task HandleMissingTimeSeriesIncludes(Dictionary<string, List<TimeSeriesRange>> missingTimeSeriesIncludes, ShardedQueryResult result)
    {
        var commandsPerShard = new Dictionary<int, GetMultipleTimeSeriesRangesCommand>();

        var shardsToPositions = ShardLocator.GetDocumentIdsByShards(Context, RequestHandler.DatabaseContext, missingTimeSeriesIncludes.Keys);

        foreach (var (shardNumber, idsByShard) in shardsToPositions)
        {
            var rangesForShard = new Dictionary<string, List<TimeSeriesRange>>();

            foreach (var id in idsByShard.Ids)
            {
                rangesForShard[id] = missingTimeSeriesIncludes[id];
            }

            commandsPerShard[shardNumber] = new GetMultipleTimeSeriesRangesCommand(rangesForShard);
        }

        var timeSeriesIncludes = await RequestHandler.ShardExecutor.ExecuteParallelForShardsAsync(shardsToPositions.Keys.ToArray(),
            new ShardedTimeSeriesOperation(RequestHandler.HttpContext, commandsPerShard), Token);

        foreach (var tsInclude in timeSeriesIncludes.Results)
        {
            if (tsInclude == null)
                continue;

            ((ShardedTimeSeriesIncludes)result.GetTimeSeriesIncludes()).AddMissingTimeSeries(tsInclude.Id, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(tsInclude.Values, Context));
        }

    }

    private void ProjectAfterMapReduce(ref ShardedQueryResult result, QueryTimingsScope scope)
    {
        if (IsProjectionFromMapReduceIndex == false)
            return;

        using (scope?.For(nameof(QueryTimingsScope.Names.Projection)))
        {
            var fieldsToFetch = new FieldsToFetch(Query, null);
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
