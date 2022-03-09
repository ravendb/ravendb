using System.Collections.Generic;
using Raven.Client;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Indexes.MapReduce.Static.Sharding;

public class ShardedAggregatedAnonymousObjects : AggregatedAnonymousObjects
{
    private static readonly DynamicJsonValue DummyDynamicJsonValue = new();

    public ShardedAggregatedAnonymousObjects(List<object> results, IPropertyAccessor propertyAccessor, JsonOperationContext indexContext) : base(results, propertyAccessor, indexContext)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "handle metadata merge, score, distance");
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "test this");

        ModifyOutputToStore = djv =>
        {
            djv[Constants.Documents.Metadata.Key] = DummyDynamicJsonValue;
        };
    }
}
