using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Server.Documents.Includes.Sharding;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Sharding;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Queries;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Operations;

public class ShardedQueryOperation : IShardedReadOperation<QueryResult, ShardedQueryResult>
{
    private readonly IndexQueryServerSide _query;
    private readonly TransactionOperationContext _context;
    private readonly ShardedDatabaseRequestHandler _requestHandler;
    private readonly Dictionary<int, ShardedQueryCommand> _queryCommands;
    private readonly ShardedDocumentsComparer _sortingComparer;
    private long _combinedResultEtag;

    public ShardedQueryOperation(IndexQueryServerSide query, TransactionOperationContext context, ShardedDatabaseRequestHandler requestHandler, Dictionary<int, ShardedQueryCommand> queryCommands, ShardedDocumentsComparer sortingComparer, string expectedEtag)
    {
        _query = query;
        _context = context;
        _requestHandler = requestHandler;
        _queryCommands = queryCommands;
        _sortingComparer = sortingComparer;
        ExpectedEtag = expectedEtag;
    }

    public string ExpectedEtag { get; }

    public HttpRequest HttpRequest { get => _requestHandler.HttpContext.Request; }

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

        foreach (var cmdResult in results.Values)
        {
            var queryRes = cmdResult.Result;
            result.TotalResults += queryRes.TotalResults;
            result.IsStale |= queryRes.IsStale;
            result.SkippedResults += queryRes.SkippedResults;
            result.IndexName = queryRes.IndexName;
            result.IncludedPaths = queryRes.IncludedPaths;

            if (result.IndexTimestamp < queryRes.IndexTimestamp) 
                result.IndexTimestamp = queryRes.IndexTimestamp;

            if (result.LastQueryTime < queryRes.LastQueryTime) 
                result.LastQueryTime = queryRes.LastQueryTime;

            if (queryRes.RaftCommandIndex.HasValue)
            {
                if (result.RaftCommandIndex == null || queryRes.RaftCommandIndex > result.RaftCommandIndex)
                    result.RaftCommandIndex = queryRes.RaftCommandIndex;
            }

            // For includes, we send the includes to all shards, then we merge them together. We do explicitly
            // support including from another shard, so we'll need to do that again for missing includes
            // That means also recording the include() call from JS on missing values that we'll need to rerun on
            // other shards

            if (queryRes.Includes is { Count: > 0 })
            {
                result.Includes ??= new List<BlittableJsonReaderObject>();

                HandleDocumentIncludes(queryRes, ref result);
            }

            if (queryRes.RevisionIncludes is {Length: > 0})
            {
                revisionIncludes ??= new ShardedRevisionIncludes();

                revisionIncludes.AddResults(queryRes.RevisionIncludes, _context);
            }

            if (queryRes.CounterIncludes != null)
            {
                counterIncludes ??= new ShardedCounterIncludes();

                counterIncludes.AddResults(queryRes.CounterIncludes, queryRes.IncludedCounterNames, _context);
            }

            if (queryRes.TimeSeriesIncludes != null)
            {
                timeSeriesIncludes ??= new ShardedTimeSeriesIncludes(true);

                timeSeriesIncludes.AddResults(queryRes.TimeSeriesIncludes, _context);
            }

            if (queryRes.CompareExchangeValueIncludes != null)
            {
                compareExchangeValueIncludes ??= new ShardedCompareExchangeValueInclude();

                compareExchangeValueIncludes.AddResults(queryRes.CompareExchangeValueIncludes, _context);
            }

            if (_sortingComparer == null)
            {
                foreach (BlittableJsonReaderObject item in queryRes.Results)
                {
                    result.Results.Add(item.Clone(_context));
                }
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

        if (_sortingComparer != null)
        {
            // all the results from each command are already ordered

            using (var mergedEnumerator = new MergedEnumerator<BlittableJsonReaderObject>(_sortingComparer))
            {
                foreach (var cmdResult in results.Values)
                {
                    mergedEnumerator.AddEnumerator(GetEnumerator(cmdResult.Result.Results));
                }

                while (mergedEnumerator.MoveNext())
                {
                    result.Results.Add(mergedEnumerator.Current?.Clone(_context));
                }

                static IEnumerator<BlittableJsonReaderObject> GetEnumerator(BlittableJsonReaderArray array)
                {
                    foreach (BlittableJsonReaderObject item in array)
                    {
                        yield return item;
                    }
                }
            }
        }

        result.RegisterSpatialProperties(_query);

        return result;
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
