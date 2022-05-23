using System.Collections.Generic;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.ServerWide;

public class SubscriptionsClusterStorage
{
    private readonly ClusterStateMachine _cluster;
    public SubscriptionsClusterStorage(ClusterStateMachine cluster)
    {
        _cluster = cluster;
    }
    
    public SubscriptionState ReadSubscriptionStateByName<T>(TransactionOperationContext<T> context, string databaseName, string name) where T : RavenTransaction
    {
        var subscriptionBlittable = ReadSubscriptionStateRaw(context, databaseName, name);

        if (subscriptionBlittable == null)
            throw new SubscriptionDoesNotExistException($"Subscription with name '{name}' was not found in server store");

        var subscriptionState = JsonDeserializationClient.SubscriptionState(subscriptionBlittable);
        return subscriptionState;
    }
    
    public SubscriptionState ReadSubscriptionStateById(TransactionOperationContext context, string databaseName, long id)
    {
        var name = GetSubscriptionNameById(context, databaseName, id);
        if (string.IsNullOrEmpty(name))
            throw new SubscriptionDoesNotExistException($"Subscription with id '{id}' was not found in server store");

        return ReadSubscriptionStateByName(context, databaseName, name);
    }

    public string GetSubscriptionNameById(TransactionOperationContext context, string databaseName, long id)
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

    public BlittableJsonReaderObject ReadSubscriptionStateRaw<T>(TransactionOperationContext<T> context, string databaseName, string name) where T : RavenTransaction
    {
        var subscriptionBlittable = _cluster.Read(context, SubscriptionState.GenerateSubscriptionItemKeyName(databaseName, name));
        return subscriptionBlittable;
    }

    public static IEnumerable<SubscriptionState> GetAllSubscriptionsWithoutState(TransactionOperationContext serverStoreContext, string database, int start, int take)
    {
        foreach (var keyValue in ClusterStateMachine.ReadValuesStartingWith(serverStoreContext, SubscriptionState.SubscriptionPrefix(database)))
        {
            if (start > 0)
            {
                start--;
                continue;
            }

            if (take-- <= 0)
                yield break;

            var subscriptionState = JsonDeserializationClient.SubscriptionState(keyValue.Value);
            yield return subscriptionState;
        }
    }
}
