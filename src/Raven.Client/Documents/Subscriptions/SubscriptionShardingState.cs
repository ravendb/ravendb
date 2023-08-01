using System.Collections.Generic;
using Raven.Client.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Subscriptions;

public sealed class SubscriptionShardingState : IDynamicJson
{
    public Dictionary<string, string> ChangeVectorForNextBatchStartingPointPerShard { get; set; }
    public Dictionary<string, string> NodeTagPerShard { get; set; }
    public Dictionary<int, string> ProcessedChangeVectorPerBucket { get; set; }
    public string ChangeVectorForNextBatchStartingPointForOrchestrator { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ChangeVectorForNextBatchStartingPointPerShard)] = ChangeVectorForNextBatchStartingPointPerShard?.ToJson(),
            [nameof(ProcessedChangeVectorPerBucket)] = ProcessedChangeVectorPerBucket?.ToJsonWithPrimitiveKey(),
            [nameof(NodeTagPerShard)] = NodeTagPerShard?.ToJson(),
            [nameof(ChangeVectorForNextBatchStartingPointForOrchestrator)] = ChangeVectorForNextBatchStartingPointForOrchestrator
        };
    }
}
