using System;
using Raven.Client.Documents.DataArchival;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Server.Config;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.ServerWide;

public sealed class SubscriptionsClusterStorage
{
    private readonly ClusterStateMachine _cluster;

    private static ArchivedDataProcessingBehavior? ArchivedDataBehaviorFromDbConfiguration;

    public SubscriptionsClusterStorage(ClusterStateMachine cluster)
    {
        _cluster = cluster;
    }

    [Obsolete($"This method should not be used directly. Use the one from '{nameof(AbstractSubscriptionStorage<AbstractSubscriptionConnectionsState>)}'.")]
    public SubscriptionState ReadSubscriptionStateByName(ClusterOperationContext context, string databaseName, string name)
    {
        var subscriptionBlittable = ReadSubscriptionStateRaw(context, databaseName, name);

        if (subscriptionBlittable == null)
            throw new SubscriptionDoesNotExistException($"Subscription with name '{name}' was not found in server store");

        var subscriptionState = JsonDeserializationClient.SubscriptionState(subscriptionBlittable);
        
        if (subscriptionState.ArchivedDataProcessingBehavior.HasValue)
            return subscriptionState;
        
        // persisted state from v5.x
        if (ArchivedDataBehaviorFromDbConfiguration.HasValue == false) 
        { 
            if(Enum.TryParse(_cluster.ReadDatabase(context, databaseName).Settings[RavenConfiguration.GetKey(x => x.Subscriptions.ArchivedDataProcessingBehavior)], false, out ArchivedDataProcessingBehavior behavior) == false)
            {
                throw new InvalidOperationException(
                    $"Failed to fetch {nameof(RavenConfiguration.Subscriptions.ArchivedDataProcessingBehavior)} from subscriptions configuration in database settings.");
            }
            ArchivedDataBehaviorFromDbConfiguration = behavior;
        }

        subscriptionState.ArchivedDataProcessingBehavior = ArchivedDataBehaviorFromDbConfiguration.Value; 
        return subscriptionState;     
    }

    [Obsolete($"This method should not be used directly. Use the one from '{nameof(AbstractSubscriptionStorage<AbstractSubscriptionConnectionsState>)}'.")]
    public string GetSubscriptionNameById(ClusterOperationContext context, string databaseName, long id)
    {
        foreach (var keyValue in ClusterStateMachine.ReadValuesStartingWith(context, SubscriptionState.SubscriptionPrefix(databaseName)))
        {
            if (keyValue.Value.TryGet(nameof(SubscriptionState.SubscriptionId), out long _id) == false)
                continue;
            if (_id == id)
            {
                if (keyValue.Value.TryGet(nameof(SubscriptionState.SubscriptionName), out string name))
                    return name;
            }
        }

        return null;
    }

    [Obsolete($"This method should not be used directly. Use the one from '{nameof(AbstractSubscriptionStorage<AbstractSubscriptionConnectionsState>)}'.")]
    public BlittableJsonReaderObject ReadSubscriptionStateRaw(ClusterOperationContext context, string databaseName, string name)
    {
        var subscriptionBlittable = _cluster.Read(context, SubscriptionState.GenerateSubscriptionItemKeyName(databaseName, name));
        return subscriptionBlittable;
    }
}
