using System.Collections.Generic;
using System.Threading;
using Raven.Client;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.MapReduce.Static.Sharding;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Queries.IndexEntries;

public sealed class ShardedMapReduceIndexEntriesQueryResultsMerger : ShardedMapReduceQueryResultsMerger
{
    private static readonly ShardedAutoMapReduceIndexResultsAggregatorForIndexEntries Aggregator = new();

    private string _reduceKeyHash;

    public ShardedMapReduceIndexEntriesQueryResultsMerger(
        List<BlittableJsonReaderObject> currentResults,
        ShardedDatabaseContext.ShardedIndexesContext indexesContext,
        string indexName,
        bool isAutoMapReduceQuery,
        TransactionOperationContext context,
        CancellationToken token)
        : base(currentResults, indexesContext, indexName, isAutoMapReduceQuery, context, token)
    {
    }

    internal override AggregationResult AggregateForAutoMapReduce(AutoMapReduceIndexDefinition indexDefinition)
    {
        BlittableJsonReaderObject currentlyProcessedResult = null;
        return Aggregator.AggregateOn(CurrentResults, indexDefinition, Context, null, ref currentlyProcessedResult, Token);
    }

    protected override List<BlittableJsonReaderObject> AggregateForStaticMapReduce(IndexInformationHolder index)
    {
        if (CurrentResults.Count != 0)
        {
            var json = CurrentResults[0];
            json.TryGet(Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName, out _reduceKeyHash);
        }

        return base.AggregateForStaticMapReduce(index);
    }

    protected override AggregatedAnonymousObjects CreateShardedAggregatedAnonymousObjects(List<object> results, IPropertyAccessor propertyAccessor, bool skipImplicitNullInOutput = false)
        => new ShardedAggregatedAnonymousObjectsForIndexEntries(results, propertyAccessor, _reduceKeyHash, Context);
}
