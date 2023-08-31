using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Sharding;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Sharding.Commands.Querying;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations.Queries;

public sealed class ShardedIndexEntriesQueryOperation : AbstractShardedQueryOperation<ShardedIndexEntriesQueryResult, BlittableJsonReaderObject, BlittableJsonReaderObject>
{
    private readonly IndexQueryServerSide _query;
    private readonly IComparer<BlittableJsonReaderObject> _sortingComparer;

    public ShardedIndexEntriesQueryOperation([NotNull] IndexQueryServerSide query,
        TransactionOperationContext context,
        ShardedDatabaseRequestHandler requestHandler,
        Dictionary<int, ShardedQueryCommand> queryCommands,
        [NotNull] IComparer<BlittableJsonReaderObject> sortingComparer,
        string expectedEtag)
        : base(query.Metadata, queryCommands, context, requestHandler, expectedEtag)
    {
        _query = query ?? throw new ArgumentNullException(nameof(query));
        _sortingComparer = sortingComparer ?? throw new ArgumentNullException(nameof(sortingComparer));
    }

    public bool FromStudio => HttpRequest.IsFromStudio();

    public override ShardedIndexEntriesQueryResult CombineResults(Dictionary<int, ShardExecutionResult<QueryResult>> results)
    {
        var result = new ShardedIndexEntriesQueryResult
        {
            Results = new List<BlittableJsonReaderObject>(),
            ResultEtag = CombinedResultEtag,
            IsStale = HadActiveMigrationsBeforeQueryStarted
        };

        foreach (var (_, cmdResult) in results)
        {
            var queryResult = cmdResult.Result;

            CombineSingleShardResultProperties(result, queryResult);
        }

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

                    yield return item.AddToMetadata(Context, Constants.Documents.Metadata.Sharding.ShardNumber, shardNumber);
                }
            }
        }

        result.RegisterSpatialProperties(_query);

        return result;
    }
}
