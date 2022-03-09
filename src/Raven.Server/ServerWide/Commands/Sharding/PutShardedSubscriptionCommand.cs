using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Sharding;

public class PutShardedSubscriptionCommand : PutSubscriptionCommand
{
    public Dictionary<string, string> InitialChangeVectorsCollection;

    public PutShardedSubscriptionCommand()
    {

    }
    public PutShardedSubscriptionCommand(string databaseName, string query, string mentor, string uniqueRequestId) : base(databaseName, query, mentor,
        uniqueRequestId)
    {
    }

    protected override void HandleChangeVectorOnUpdate(ClusterOperationContext context, SubscriptionState existingSubscriptionState, long subscriptionId)
    {
        if (InitialChangeVector == nameof(Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange))
        {
            // use current change vectors saved in state
            InitialChangeVectorsCollection = existingSubscriptionState.NextBatchStartingPointChangeVectors;
            return;
        }

        if (InitialChangeVectorsCollection == null || InitialChangeVectorsCollection.Count == 0)
            return;

        // start from LastDocument (the CVs were validated in handler)
        InitialChangeVectorsCollection = existingSubscriptionState.NextBatchStartingPointChangeVectors;
        // remove the old state from storage
        RemoveSubscriptionStateFromStorage(context, subscriptionId);
    }

    protected override void AssertValidChangeVector()
    {
        // the CVs were validated in handler
    }

    protected override DynamicJsonValue Builder(long subscriptionId)
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
            NextBatchStartingPointChangeVectors = InitialChangeVectorsCollection
        }.ToJson();
    }

    public override void FillJson(DynamicJsonValue json)
    {
        base.FillJson(json);
        json[nameof(InitialChangeVectorsCollection)] = InitialChangeVectorsCollection?.ToJson();
    }
}
