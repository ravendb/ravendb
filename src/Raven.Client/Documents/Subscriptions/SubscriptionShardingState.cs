using System.Collections.Generic;
using Raven.Client.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Subscriptions;

/// <summary>
/// Represents sharding-related state for a subscription, including per-shard change vectors and node assignments.
/// </summary>
public sealed class SubscriptionShardingState : IDynamicJson
{
    /// <summary>
        /// Next-batch starting point change vector per shard.
        /// </summary>
        public Dictionary<string, string> ChangeVectorForNextBatchStartingPointPerShard { get; set; }
    /// <summary>
        /// Node tag per shard that is responsible for processing it.
        /// </summary>
        public Dictionary<string, string> NodeTagPerShard { get; set; }
    /// <summary>
        /// Last processed change vector per bucket (for bucketed sharding).
        /// </summary>
        public Dictionary<int, string> ProcessedChangeVectorPerBucket { get; set; }
    /// <summary>
        /// Next-batch starting point change vector as tracked by the orchestrator node.
        /// </summary>
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
