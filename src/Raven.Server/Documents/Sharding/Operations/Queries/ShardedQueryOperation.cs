﻿using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Raven.Server.Documents.Includes.Sharding;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Sharding;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Sharding.Commands.Querying;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Operations.Queries;

public class ShardedQueryOperation : AbstractShardedQueryOperation<ShardedQueryResult, BlittableJsonReaderObject, BlittableJsonReaderObject>
{
    private readonly IndexQueryServerSide _query;
    private readonly IComparer<BlittableJsonReaderObject> _sortingComparer;

    public ShardedQueryOperation(IndexQueryServerSide query,
        TransactionOperationContext context,
        ShardedDatabaseRequestHandler requestHandler,
        Dictionary<int, ShardedQueryCommand> queryCommands,
        IComparer<BlittableJsonReaderObject> sortingComparer, string expectedEtag)
        : base(queryCommands, context, requestHandler, expectedEtag)
    {
        _query = query;
        _sortingComparer = sortingComparer ?? new RoundRobinComparer();
    }

    public bool FromStudio => HttpRequest.IsFromStudio();

    public HashSet<string> MissingCounterIncludes { get; private set; }

    public Dictionary<string, List<TimeSeriesRange>> MissingTimeSeriesIncludes { get; set; }

    public override ShardedQueryResult CombineResults(Dictionary<int, ShardExecutionResult<QueryResult>> results)
    {
        var result = new ShardedQueryResult
        {
            Results = new List<BlittableJsonReaderObject>(),
            ResultEtag = CombinedResultEtag
        };

        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "Check if we could handle this in streaming manner so we won't need to materialize all blittables and do Clone() here");

        ShardedRevisionIncludes revisionIncludes = null;
        ShardedCounterIncludes counterIncludes = null;
        ShardedCompareExchangeValueInclude compareExchangeValueIncludes = null;
        ShardedTimeSeriesIncludes timeSeriesIncludes = null;

        foreach (var (shardNumber, cmdResult) in results)
        {
            var command = QueryCommands[shardNumber];
            var queryResult = cmdResult.Result;

            CombineExplanations(cmdResult);

            command.Scope?.WithBase(cmdResult.Result.Timings);

            CombineSingleShardResultProperties(result, queryResult);

            // For includes, we send the includes to all shards, then we merge them together. We do explicitly
            // support including from another shard, so we'll need to do that again for missing includes
            // That means also recording the include() call from JS on missing values that we'll need to rerun on
            // other shards

            if (queryResult.Includes is { Count: > 0 })
            {
                result.Includes ??= new List<BlittableJsonReaderObject>();

                HandleDocumentIncludes(queryResult, result);
            }

            if (queryResult.RevisionIncludes is { Length: > 0 })
            {
                revisionIncludes ??= new ShardedRevisionIncludes();

                revisionIncludes.AddResults(queryResult.RevisionIncludes, Context);
            }

            if (queryResult.CounterIncludes != null)
            {
                counterIncludes ??= new ShardedCounterIncludes();

                counterIncludes.AddResults(queryResult.CounterIncludes, queryResult.IncludedCounterNames, Context);
            }

            if (queryResult.TimeSeriesIncludes != null)
            {
                timeSeriesIncludes ??= new ShardedTimeSeriesIncludes(true);

                timeSeriesIncludes.AddResults(queryResult.TimeSeriesIncludes, Context);
            }

            if (queryResult.CompareExchangeValueIncludes != null)
            {
                compareExchangeValueIncludes ??= new ShardedCompareExchangeValueInclude();

                compareExchangeValueIncludes.AddResults(queryResult.CompareExchangeValueIncludes, Context);
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
                mergedEnumerator.AddEnumerator(GetEnumerator(cmdResult.Result.Results.Clone(Context), shardNumber));
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

                    yield return item.AddToMetadata(Context, Constants.Documents.Metadata.ShardNumber, shardNumber);
                }
            }
        }

        result.RegisterSpatialProperties(_query);

        return result;

        void CombineExplanations(ShardExecutionResult<QueryResult> cmdResult)
        {
            if (cmdResult.Result.Explanations is not { Count: > 0 }) 
                return;

            result.Explanations ??= new Dictionary<string, string[]>();

            foreach (var kvp in cmdResult.Result.Explanations)
                result.Explanations[kvp.Key] = kvp.Value;
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
}
