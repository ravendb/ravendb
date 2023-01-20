using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries;
using Raven.Client.Util;
using Raven.Server.Documents.Includes.Sharding;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Results.Sharding;
using Raven.Server.Documents.Queries.Sharding;
using Raven.Server.Documents.Sharding.Commands.Querying;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Handlers.Processors.Counters;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Queries;

/// <summary>
/// A struct that we use to hold state and break the process
/// of handling a sharded query into distinct steps
/// </summary>
public class ShardedQueryProcessor : AbstractShardedQueryProcessor<ShardedQueryCommand, QueryResult, ShardedQueryResult>
{
    private readonly long? _existingResultEtag;

    private readonly string _raftUniqueRequestId;
    private readonly HashSet<int> _filteredShardIndexes;

    // User should also be able to define a query parameter ("__shardContext") which is an array
    // that contains the ids whose shards the query should be limited to. Advanced: Optimization if user
    // want to run a query and knows what shards it is on. Such as:
    // from Orders where State = $state and User = $user where all the orders are on the same share as the user

    public ShardedQueryProcessor(TransactionOperationContext context, ShardedDatabaseRequestHandler requestHandler, IndexQueryServerSide query, long? existingResultEtag, bool metadataOnly, bool indexEntriesOnly,
        CancellationToken token) : base(context, requestHandler, query, metadataOnly, indexEntriesOnly, token)
    {
        _existingResultEtag = existingResultEtag;
        _raftUniqueRequestId = _requestHandler.GetRaftRequestIdFromQuery() ?? RaftIdGenerator.NewId();

        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "RavenDB-19084 Add an option to select the shards for query in the client");
        
        if (_query.QueryParameters != null && _query.QueryParameters.TryGetMember("__shardContext", out object filter))
        {
            _filteredShardIndexes = new HashSet<int>();
            switch (filter)
            {
                case LazyStringValue id:
                    _filteredShardIndexes.Add(_requestHandler.DatabaseContext.GetShardNumberFor(context, id));
                    break;
                case BlittableJsonReaderArray arr:
                {
                    for (int i = 0; i < arr.Length; i++)
                    {
                        var it = arr.GetStringByIndex(i);
                        _filteredShardIndexes.Add(_requestHandler.DatabaseContext.GetShardNumberFor(context, it));
                    }
                    break;
                }
            }
        }
        else
        {
            _filteredShardIndexes = null;
        }
    }

    protected override ShardedQueryCommand CreateCommand(BlittableJsonReaderObject query)
    {
        return new ShardedQueryCommand(_context.ReadObject(query, "query"), _query, _metadataOnly, _indexEntriesOnly, _query.Metadata.IndexName,
            canReadFromCache: _existingResultEtag != null, raftUniqueRequestId: _raftUniqueRequestId);
    }

    public override async Task<ShardedQueryResult> ExecuteShardedOperations()
    {
        ShardedDocumentsComparer documentsComparer = null;

        if (_query.Metadata.OrderBy?.Length > 0 && (_isMapReduceIndex || _isAutoMapReduceQuery) == false)
        {
            // sorting only if:
            // 1. we have fields to sort
            // 2. it isn't a map-reduce index/query (the sorting will be done after the re-reduce)
            documentsComparer = new ShardedDocumentsComparer(_query.Metadata, _isMapReduceIndex || _isAutoMapReduceQuery);
        }

        if (_query.Metadata.TimeSeriesIncludes != null)
        {
            var timeSeriesKeys = _query.Metadata.TimeSeriesIncludes.TimeSeries.Keys;
        }

        var commands = GetOperationCommands();

        var operation = new ShardedQueryOperation(_query, _context, _requestHandler, commands, documentsComparer, _existingResultEtag?.ToString());

        int[] shards = _filteredShardIndexes == null ? commands.Keys.ToArray() : commands.Keys.Intersect(_filteredShardIndexes).ToArray();
        var shardedReadResult = await _requestHandler.ShardExecutor.ExecuteParallelForShardsAsync(shards, operation, _token);

        if (shardedReadResult.StatusCode == (int)HttpStatusCode.NotModified)
        {
            return new ShardedQueryResult { NotModified = true };
        }

        var result = shardedReadResult.Result;

        if (_isAutoMapReduceQuery && result.RaftCommandIndex.HasValue)
        {
            // we are waiting here for all nodes, we should wait for all of the orchestrators at least to apply that
            // so further queries would not throw index does not exist in case of a failover
            await _requestHandler.DatabaseContext.Cluster.WaitForExecutionOnAllNodesAsync(result.RaftCommandIndex.Value);
        }

        if (operation.MissingDocumentIncludes is {Count: > 0})
        {
            await HandleMissingDocumentIncludes(operation.MissingDocumentIncludes, result);
        }

        if (operation.MissingCounterIncludes is {Count: > 0})
        {
            await HandleMissingCounterInclude(operation.MissingCounterIncludes, result);
        }

        if (operation.MissingTimeSeriesIncludes is {Count: > 0})
        {
            await HandleMissingTimeSeriesIncludes(operation.MissingTimeSeriesIncludes, result);
        }

        // For map/reduce - we need to re-run the reduce portion of the index again on the results
        ReduceResults(ref result);

        ApplyPaging(ref result);

        // For map-reduce indexes we project the results after the reduce part 
        ProjectAfterMapReduce(ref result);

        // * For JS projections and load clauses, we don't support calling load() on a
        //   document that is not on the same shard
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "RavenDB-17889 Add a test for that");

        return result;
    }

    private async Task HandleMissingDocumentIncludes(HashSet<string> missingIncludes, ShardedQueryResult result)
    {
        var missingIncludeIdsByShard = ShardLocator.GetDocumentIdsByShards(_context, _requestHandler.DatabaseContext, missingIncludes);
        var missingIncludesOp = new FetchDocumentsFromShardsOperation(_context, _requestHandler, missingIncludeIdsByShard, null, null, counterIncludes: default, null, null, null, _metadataOnly);
        var missingResult = await _requestHandler.DatabaseContext.ShardExecutor.ExecuteParallelForShardsAsync(missingIncludeIdsByShard.Keys.ToArray(), missingIncludesOp, _token);

        foreach (var (_, missing) in missingResult.Result.Documents)
        {
            if (missing == null)
                continue;

            result.Includes.Add(missing);
        }
    }

    private async Task HandleMissingCounterInclude(HashSet<string> missingCounterIncludes, ShardedQueryResult result)
    {
        var counterBatch = new CounterBatch();

        foreach (string docId in missingCounterIncludes)
        {
            var includes = result.GetCounterIncludes();

            var  counterOperations = new List<CounterOperation>();

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

        var shardsToPositions = ShardLocator.GetDocumentIdsByShards(_context, _requestHandler.DatabaseContext, missingCounterIncludes);
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

        var counterIncludes = await _requestHandler.ShardExecutor.ExecuteParallelForShardsAsync(shardsToPositions.Keys.ToArray(),
            new ShardedCounterBatchOperation(_requestHandler.HttpContext, commandsPerShard), _token);

        foreach (var counterInclude in counterIncludes.Counters)
        {
            if (counterInclude == null)
                continue;

            ((ShardedCounterIncludes)result.GetCounterIncludes()).AddMissingCounter(counterInclude.DocumentId, _context.ReadObject(counterInclude.ToJson(), counterInclude.DocumentId));
        }
    }
    private async Task HandleMissingTimeSeriesIncludes(Dictionary<string, List<TimeSeriesRange>> missingTimeSeriesIncludes, ShardedQueryResult result)
    {
        var commandsPerShard = new Dictionary<int, GetMultipleTimeSeriesRangesCommand>();

        var shardsToPositions = ShardLocator.GetDocumentIdsByShards(_context, _requestHandler.DatabaseContext, missingTimeSeriesIncludes.Keys);

        foreach (var (shardNumber, idsByShard) in shardsToPositions)
        {
            var rangesForShard = new Dictionary<string, List<TimeSeriesRange>>();

            foreach (var id in idsByShard.Ids)
            {
                rangesForShard[id] = missingTimeSeriesIncludes[id];
            }

            commandsPerShard[shardNumber] = new GetMultipleTimeSeriesRangesCommand(rangesForShard);
        }

        var timeSeriesIncludes = await _requestHandler.ShardExecutor.ExecuteParallelForShardsAsync(shardsToPositions.Keys.ToArray(),
            new ShardedTimeSeriesOperation(_requestHandler.HttpContext, commandsPerShard), _token);

        foreach (var tsInclude in timeSeriesIncludes.Results)
        {
            if (tsInclude == null)
                continue;

            ((ShardedTimeSeriesIncludes)result.GetTimeSeriesIncludes()).AddMissingTimeSeries(tsInclude.Id, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(tsInclude.Values, _context));
        }

    }

    private void ApplyPaging(ref ShardedQueryResult result)
    {
        if (_query.Offset is > 0 && result.Results.Count > _query.Offset)
        {
            var count = Math.Min(_query.Offset ?? 0, int.MaxValue);
            result.Results.RemoveRange(0, (int)count);
        }

        if (_query.Limit is > 0 && result.Results.Count > _query.Limit)
        {
            var index = Math.Min(_query.Limit.Value, int.MaxValue);
            var count = result.Results.Count - _query.Limit.Value;
            if (count > int.MaxValue)
                count = int.MaxValue; //todo: Grisha: take a look
            result.Results.RemoveRange((int)index, (int)count);
        }
    }

    private void ReduceResults(ref ShardedQueryResult result)
    {
        if (_isMapReduceIndex || _isAutoMapReduceQuery)
        {
            var merger = new ShardedMapReduceQueryResultsMerger(result.Results, _requestHandler.DatabaseContext.Indexes, result.IndexName, _isAutoMapReduceQuery, _context);
            result.Results = merger.Merge();

            if (_query.Metadata.OrderBy?.Length > 0 && (_isMapReduceIndex || _isAutoMapReduceQuery))
            {
                // apply ordering after the re-reduce of a map-reduce index
                result.Results.Sort(new ShardedDocumentsComparer(_query.Metadata, isMapReduce: true));
            }
        }
    }

    private void ProjectAfterMapReduce(ref ShardedQueryResult result)
    {
        if (_isMapReduceIndex == false && _isAutoMapReduceQuery == false
            || (_query.Metadata.Query.Select == null || _query.Metadata.Query.Select.Count == 0)
            && _query.Metadata.Query.SelectFunctionBody.FunctionText == null)
            return;

        var fieldsToFetch = new FieldsToFetch(_query, null);
        var retriever = new ShardedMapReduceResultRetriever(_requestHandler.DatabaseContext.Indexes.ScriptRunnerCache, _query, null, SearchEngineType.Lucene, fieldsToFetch, null, _context, false, null, null, null,
            _requestHandler.DatabaseContext.IdentityPartsSeparator);

        var currentResults = result.Results;
        result.Results = new List<BlittableJsonReaderObject>();

        foreach (var data in currentResults)
        {
            var retrieverInput = new RetrieverInput();
            (Document document, List<Document> documents) = retriever.GetProjectionFromDocument(new Document
            {
                Data = data
            }, ref retrieverInput, fieldsToFetch, _context, CancellationToken.None);
            
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
                var doc = _context.ReadObject(documentData, "modified-map-reduce-result");
                results.Add(doc);
            }
        }
    }
}
