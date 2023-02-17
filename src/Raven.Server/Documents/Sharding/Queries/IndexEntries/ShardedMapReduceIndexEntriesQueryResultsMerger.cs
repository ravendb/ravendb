using System.Collections.Generic;
using System.Threading;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Queries.IndexEntries;

public class ShardedMapReduceIndexEntriesQueryResultsMerger : ShardedMapReduceQueryResultsMerger
{
    private static readonly ShardedAutoMapReduceIndexResultsAggregatorForIndexEntries Aggregator = new();

    public ShardedMapReduceIndexEntriesQueryResultsMerger(
        List<BlittableJsonReaderObject> currentResults,
        ShardedDatabaseContext.ShardedIndexesContext indexesContext,
        string indexName,
        bool isAutoMapReduceQuery,
        TransactionOperationContext context)
        : base(currentResults, indexesContext, indexName, isAutoMapReduceQuery, context)
    {
    }

    protected override AggregationResult AggregateForAutoMapReduce(AutoMapReduceIndexDefinition indexDefinition)
    {
        BlittableJsonReaderObject currentlyProcessedResult = null;
        return Aggregator.AggregateOn(CurrentResults, indexDefinition, Context, null, ref currentlyProcessedResult, CancellationToken.None);
    }

    protected override List<BlittableJsonReaderObject> AggregateForStaticMapReduce(IndexInformationHolder index)
    {
        return base.AggregateForStaticMapReduce(index);
    }
}
