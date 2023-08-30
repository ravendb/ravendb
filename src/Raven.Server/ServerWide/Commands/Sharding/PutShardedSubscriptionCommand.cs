using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Sharding;

public sealed class PutShardedSubscriptionCommand : PutSubscriptionCommand
{
    public Dictionary<string, string> InitialChangeVectorPerShard;

    private PutShardedSubscriptionCommand()
    {

    }

    public PutShardedSubscriptionCommand(string databaseName, string query, string mentor, string uniqueRequestId) : base(databaseName, query, mentor,
        uniqueRequestId)
    {
    }

    protected override void HandleChangeVectorOnUpdate(ClusterOperationContext context, SubscriptionState existingSubscriptionState, long subscriptionId)
    {
        var existingShardsCVs = existingSubscriptionState.ShardingState.ChangeVectorForNextBatchStartingPointPerShard;

        if (InitialChangeVector == nameof(Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange))
        {
            // use current change vectors saved in state
            InitialChangeVectorPerShard = existingShardsCVs;
            return;
        }

        if (InitialChangeVectorPerShard == null || InitialChangeVectorPerShard.Count == 0 || InitialChangeVectorPerShard.All(x => string.IsNullOrEmpty(x.Value)))
        {
            InitialChangeVectorPerShard = null;
            return;
        }

        // start from LastDocument (the CVs were validated in handler) or CV set by admin
        if (CompareShardsChangeVectors(existingShardsCVs))
        {
            return;
        }
        
        // remove the old state from storage
        RemoveSubscriptionStateFromStorage(context, subscriptionId);
    }

    private bool CompareShardsChangeVectors(Dictionary<string, string> existingCVs)
    {
        if (InitialChangeVectorPerShard.Count != existingCVs.Count)
            return false;

        foreach (var key in InitialChangeVectorPerShard.Keys)
        {
            if (existingCVs.ContainsKey(key) == false)
                return false;
        }

        foreach (var kvp in InitialChangeVectorPerShard)
        {
            if (kvp.Value != existingCVs[kvp.Key])
                return false;
        }

        return true;
    }

    protected override void AssertValidChangeVector()
    {
        // the CVs were validated in handler
    }

    protected override DynamicJsonValue CreateSubscriptionStateAsJson(long subscriptionId)
    {
        return new SubscriptionState
        {
            Query = Query,
            SubscriptionId = subscriptionId,
            SubscriptionName = SubscriptionName,
            LastBatchAckTime = null,
            Disabled = Disabled,
            MentorNode = MentorNode,
            LastClientConnectionTime = null,
            ShardingState = new SubscriptionShardingState
            {
                ChangeVectorForNextBatchStartingPointPerShard = InitialChangeVectorPerShard
            },
            ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior
        }.ToJson();
    }

    public override void FillJson(DynamicJsonValue json)
    {
        base.FillJson(json);
        json[nameof(InitialChangeVectorPerShard)] = InitialChangeVectorPerShard?.ToJson();
    }
}
