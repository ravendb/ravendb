using System.Collections.Generic;
using Raven.Client;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.MapReduce.Static.Sharding;

public sealed class ShardedAggregatedAnonymousObjectsForIndexEntries : AggregatedAnonymousObjects
{
    public ShardedAggregatedAnonymousObjectsForIndexEntries(List<object> results, IPropertyAccessor propertyAccessor, string reduceKeyHash, JsonOperationContext indexContext) 
        : base(results, propertyAccessor, indexContext)
    {
        if (reduceKeyHash != null)
        {
            ModifyOutputToStore = djv =>
            {
                djv[Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName] = reduceKeyHash;
            };
        }
    }
}
