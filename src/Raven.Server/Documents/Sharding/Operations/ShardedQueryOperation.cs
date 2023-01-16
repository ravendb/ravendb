using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Server.Documents.Includes.Sharding;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Sharding;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Sharding.Commands.Querying;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Queries;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Operations;

public class ShardedQueryOperation : IShardedReadOperation<QueryResult, ShardedQueryResult>
{
    private readonly IndexQueryServerSide _query;
    private readonly TransactionOperationContext _context;
    private readonly ShardedDatabaseRequestHandler _requestHandler;
    private readonly Dictionary<int, ShardedQueryCommand> _queryCommands;
    private readonly IComparer<BlittableJsonReaderObject> _sortingComparer;
    private long _combinedResultEtag;

    public ShardedQueryOperation(IndexQueryServerSide query, TransactionOperationContext context, ShardedDatabaseRequestHandler requestHandler, Dictionary<int, ShardedQueryCommand> queryCommands, IComparer<BlittableJsonReaderObject> sortingComparer, string expectedEtag)
    {
        _query = query;
        _context = context;
        _requestHandler = requestHandler;
        _queryCommands = queryCommands;
        _sortingComparer = sortingComparer ?? new RoundRobinComparer();
        ExpectedEtag = expectedEtag;
    }

    public string ExpectedEtag { get; }

    public HttpRequest HttpRequest { get => _requestHandler.HttpContext.Request; }

    public bool FromStudio => HttpRequest.IsFromStudio();

    RavenCommand<QueryResult> IShardedOperation<QueryResult, ShardedReadResult<ShardedQueryResult>>.CreateCommandForShard(int shardNumber) => _queryCommands[shardNumber];

    public HashSet<string> MissingDocumentIncludes { get; private set; }

    public HashSet<string> MissingCounterIncludes { get; private set; }

    public Dictionary<string, List<TimeSeriesRange>> MissingTimeSeriesIncludes { get; set; }

    public string CombineCommandsEtag(Dictionary<int, ShardExecutionResult<QueryResult>> commands)
    {
        _combinedResultEtag = 0;

        foreach (var cmd in commands.Values)
        {
            _combinedResultEtag = Hashing.Combine(_combinedResultEtag, cmd.Result.ResultEtag);
        }

        return CharExtensions.ToInvariantString(_combinedResultEtag);
    }

    public ShardedQueryResult CombineResults(Dictionary<int, ShardExecutionResult<QueryResult>> results)
    {
        var result = new ShardedQueryResult
        {
            Results = new List<BlittableJsonReaderObject>(),
            ResultEtag = _combinedResultEtag
        };

        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "Check if we could handle this in streaming manner so we won't need to materialize all blittables and do Clone() here");

        ShardedRevisionIncludes revisionIncludes = null;
        ShardedCounterIncludes counterIncludes = null;
        ShardedCompareExchangeValueInclude compareExchangeValueIncludes = null;
        ShardedTimeSeriesIncludes timeSeriesIncludes = null;

        foreach (var (shardNumber, cmdResult) in results)
        {
            var queryResult = cmdResult.Result;
            
            CombineSingleShardResultProperties(result, queryResult);

            // For includes, we send the includes to all shards, then we merge them together. We do explicitly
            // support including from another shard, so we'll need to do that again for missing includes
            // That means also recording the include() call from JS on missing values that we'll need to rerun on
            // other shards

            if (queryResult.Includes is { Count: > 0 })
            {
                result.Includes ??= new List<BlittableJsonReaderObject>();

                HandleDocumentIncludes(queryResult, ref result);
            }

            if (queryResult.RevisionIncludes is {Length: > 0})
            {
                revisionIncludes ??= new ShardedRevisionIncludes();

                revisionIncludes.AddResults(queryResult.RevisionIncludes, _context);
            }

            if (queryResult.CounterIncludes != null)
            {
                counterIncludes ??= new ShardedCounterIncludes();

                counterIncludes.AddResults(queryResult.CounterIncludes, queryResult.IncludedCounterNames, _context);
            }

            if (queryResult.TimeSeriesIncludes != null)
            {
                timeSeriesIncludes ??= new ShardedTimeSeriesIncludes(true);

                timeSeriesIncludes.AddResults(queryResult.TimeSeriesIncludes, _context);
            }

            if (queryResult.CompareExchangeValueIncludes != null)
            {
                compareExchangeValueIncludes ??= new ShardedCompareExchangeValueInclude();

                compareExchangeValueIncludes.AddResults(queryResult.CompareExchangeValueIncludes, _context);
            }
        }

        if (revisionIncludes != null)
            result.AddRevisionIncludes(revisionIncludes);

        if (counterIncludes != null)
        {
            result.AddCounterIncludes(counterIncludes);
            MissingCounterIncludes = counterIncludes.MissingCounterIncludes;
        }

        if (timeSeriesIncludes != null)
        {
            result.AddTimeSeriesIncludes(timeSeriesIncludes);
            MissingTimeSeriesIncludes = timeSeriesIncludes.MissingTimeSeriesIncludes;
        }

        if (compareExchangeValueIncludes != null)
            result.AddCompareExchangeValueIncludes(compareExchangeValueIncludes);

        // all the results from each command are already ordered
        using (var mergedEnumerator = new MergedEnumerator<BlittableJsonReaderObject>(_sortingComparer))
        {
            foreach (var (shardNumber, cmdResult) in results)
            {
                mergedEnumerator.AddEnumerator(GetEnumerator(cmdResult.Result.Results.Clone(_context), shardNumber));
            }

            while (mergedEnumerator.MoveNext())
            {
                result.Results.Add(mergedEnumerator.Current);
            }

            IEnumerator<BlittableJsonReaderObject> GetEnumerator(BlittableJsonReaderArray array, int shardNumber)
            {
                foreach (BlittableJsonReaderObject item in array)
                {
                    if (FromStudio == false)
                    {
                        yield return item;
                        continue;
                    }

                    yield return item.AddToMetadata(_context, Constants.Documents.Metadata.ShardNumber, shardNumber);
                }
            }
        }

        result.RegisterSpatialProperties(_query);

        return result;
    }

    internal static void CombineSingleShardResultProperties<TResult, TInclude>(QueryResult<List<TResult>, List<TInclude>> combinedResult, QueryResult singleShardResult)
    {
        combinedResult.TotalResults += singleShardResult.TotalResults;
        combinedResult.IsStale |= singleShardResult.IsStale;
        combinedResult.SkippedResults += singleShardResult.SkippedResults;
        combinedResult.IndexName = singleShardResult.IndexName;
        combinedResult.IncludedPaths = singleShardResult.IncludedPaths;

        if (combinedResult.IndexTimestamp < singleShardResult.IndexTimestamp)
            combinedResult.IndexTimestamp = singleShardResult.IndexTimestamp;

        if (combinedResult.LastQueryTime < singleShardResult.LastQueryTime)
            combinedResult.LastQueryTime = singleShardResult.LastQueryTime;

        if (singleShardResult.RaftCommandIndex.HasValue)
        {
            if (combinedResult.RaftCommandIndex == null || singleShardResult.RaftCommandIndex > combinedResult.RaftCommandIndex)
                combinedResult.RaftCommandIndex = singleShardResult.RaftCommandIndex;
        }
    }

    private class RoundRobinComparer : IComparer<BlittableJsonReaderObject>
    {
        private long _current;

        public int Compare(BlittableJsonReaderObject _, BlittableJsonReaderObject __)
        {
            return _current++ % 2 == 0 ? 1 : -1;
        }
    }

    private void HandleDocumentIncludes(QueryResult cmdResult, ref ShardedQueryResult result)
    {
        foreach (var id in cmdResult.Includes.GetPropertyNames())
        {
            if (cmdResult.Includes.TryGet(id, out BlittableJsonReaderObject include) && include != null)
            {
                result.Includes.Add(include.Clone(_context));
            }
            else
            {
                (MissingDocumentIncludes ??= new HashSet<string>()).Add(id);
            }
        }
    }
}
