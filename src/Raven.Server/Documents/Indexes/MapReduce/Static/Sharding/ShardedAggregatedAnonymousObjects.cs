using System.Collections.Generic;
using Raven.Client;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes.MapReduce.Static.Sharding;

public class ShardedAggregatedAnonymousObjects : AggregatedAnonymousObjects
{
    private static readonly DynamicJsonValue DummyDynamicJsonValue = new();
    
    public ShardedAggregatedAnonymousObjects(List<object> results, IPropertyAccessor propertyAccessor, JsonOperationContext indexContext, bool skipImplicitNullInOutput) : base(results, propertyAccessor, indexContext)
    {
        SkipImplicitNullInOutput = skipImplicitNullInOutput;
        ModifyOutputToStore = djv =>
        {
            djv[Constants.Documents.Metadata.Key] = DummyDynamicJsonValue;
        };
    }
}
