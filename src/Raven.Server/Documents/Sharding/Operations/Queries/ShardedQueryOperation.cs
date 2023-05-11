using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Raven.Server.Documents.Includes.Sharding;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Sharding;
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
    private readonly bool _isDistinctQuery;
    private readonly IComparer<BlittableJsonReaderObject> _sortingComparer;
    private readonly HashSet<int> _alreadySeenProjections;
    private HashSet<string> _timeSeriesFieldNames;

    public ShardedQueryOperation([NotNull] IndexQueryServerSide query,
        bool isProjectionFromMapReduceIndex,
        TransactionOperationContext context,
        ShardedDatabaseRequestHandler requestHandler,
        Dictionary<int, ShardedQueryCommand> queryCommands,
        [NotNull] IComparer<BlittableJsonReaderObject> sortingComparer,
        string expectedEtag)
        : base(query.Metadata, queryCommands, context, requestHandler, expectedEtag)
    {
        _query = query ?? throw new ArgumentNullException(nameof(query));
        _sortingComparer = sortingComparer ?? throw new ArgumentNullException(nameof(sortingComparer));

        if (query.Metadata.IsDistinct && isProjectionFromMapReduceIndex == false)
        {
            _isDistinctQuery = true;
            _alreadySeenProjections = new HashSet<int>();
        }
    }

    public bool FromStudio => HttpRequest.IsFromStudio();

    public HashSet<string> MissingCounterIncludes { get; private set; }

    public Dictionary<string, List<TimeSeriesRange>> MissingTimeSeriesIncludes { get; set; }

    public override ShardedQueryResult CombineResults(Dictionary<int, ShardExecutionResult<QueryResult>> results)
    {
        var result = new ShardedQueryResult
        {
            Results = new List<BlittableJsonReaderObject>(),
            ResultEtag = CombinedResultEtag,
            IsStale = HadActiveMigrationsBeforeQueryStarted
        };

        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "Check if we could handle this in streaming manner so we won't need to materialize all blittables and do Clone() here");

        ShardedRevisionIncludes revisionIncludes = null;
        ShardedCounterIncludes counterIncludes = null;
        ShardedCompareExchangeValueInclude compareExchangeValueIncludes = null;
        ShardedTimeSeriesIncludes timeSeriesIncludes = null;

        foreach (var (shardNumber, cmdResult) in results)
        {
            var queryResult = cmdResult.Result;

            CombineExplanations(result, cmdResult);
            CombineTimings(shardNumber, cmdResult);
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

            if (_query.Metadata.HasTimeSeriesSelect)
            {
                if (QueryCommands[cmdResult.ShardNumber].RawResult != null && QueryCommands[cmdResult.ShardNumber].RawResult
                        .TryGet<BlittableJsonReaderArray>(nameof(ShardedQueryResult.TimeSeriesFields), out var timeSeriesFieldNames) && timeSeriesFieldNames.Length > 0)
                {
                    _timeSeriesFieldNames ??= new HashSet<string>(StringComparer.Ordinal);

                    foreach (object name in timeSeriesFieldNames)
                    {
                        _timeSeriesFieldNames.Add(name.ToString());
                    }
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

        if (_timeSeriesFieldNames != null)
            result.TimeSeriesFields = _timeSeriesFieldNames.ToList();

        // all the results from each command are already ordered
        using (var mergedEnumerator = new MergedEnumerator<BlittableJsonReaderObject>(_sortingComparer))
        {
            foreach (var (shardNumber, cmdResult) in results)
            {
                mergedEnumerator.AddEnumerator(GetEnumerator(cmdResult.Result.Results, shardNumber));
                result.AddToDispose(cmdResult.ContextReleaser);
                cmdResult.ContextReleaser = null;
            }

            while (mergedEnumerator.MoveNext())
            {
                result.Results.Add(mergedEnumerator.Current);
            }

            IEnumerator<BlittableJsonReaderObject> GetEnumerator(BlittableJsonReaderArray array, int shardNumber)
            {
                foreach (BlittableJsonReaderObject item in array)
                {
                    if (_isDistinctQuery)
                    {
                        if (CanIncludeResult(item) == false)
                            continue;
                    }

                    if (FromStudio == false)
                    {
                        yield return item;
                        continue;
                    }

                    yield return item.AddToMetadata(Context, Constants.Documents.Metadata.Sharding.ShardNumber, shardNumber);
                }
            }
        }

        result.RegisterSpatialProperties(_query);

        return result;
    }

    private bool CanIncludeResult(BlittableJsonReaderObject item)
    {
        if (item.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
            throw new InvalidOperationException($"Couldn't find metadata in a query result: {item}");

        if (metadata.TryGet(Constants.Documents.Metadata.Sharding.Querying.ResultDataHash, out LazyStringValue queryResultHash) == false)
            throw new InvalidOperationException($"Couldn't find {Constants.Documents.Metadata.Sharding.Querying.ResultDataHash} metadata in a query result: {item}");

        return _alreadySeenProjections.Add(queryResultHash.GetHashCode());
    }
}
