using System;
using System.Collections.Concurrent;
using System.Threading;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding;

public partial class ShardedDatabaseContext
{
    public ShardedSubscriptions Subscriptions;

    public class ShardedSubscriptions : ISubscriptionSemaphore
    {
        private readonly ShardedDatabaseContext _context;
        private readonly ServerStore _serverStore;
        private readonly string _databaseName;

        public readonly SemaphoreSlim ConcurrentConnectionsSemiSemaphore;

        public readonly ConcurrentDictionary<long, SubscriptionConnectionsStateOrchestrator> SubscriptionsConnectionsState =
            new ConcurrentDictionary<long, SubscriptionConnectionsStateOrchestrator>();

        public ShardedSubscriptions(ShardedDatabaseContext context, ServerStore serverStore)
        {
            _context = context;
            _serverStore = serverStore;
            _databaseName = context.DatabaseName;
            ConcurrentConnectionsSemiSemaphore = new SemaphoreSlim(context.Configuration.Subscriptions.MaxNumberOfConcurrentConnections);
        }

        public void Update(RawDatabaseRecord databaseRecord)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal,
                "This is almost identical as the one from the subscription storage");

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                //checks which subscriptions should be dropped because of the database record change
                foreach (var subscriptionStateKvp in SubscriptionsConnectionsState)
                {
                    var subscriptionName = subscriptionStateKvp.Value.SubscriptionName;
                    if (subscriptionName == null)
                        continue;

                    var id = subscriptionStateKvp.Key;
                    var subscriptionConnectionsState = subscriptionStateKvp.Value;

                    using var subscriptionStateRaw = _serverStore.Cluster.Subscriptions.ReadSubscriptionStateRaw(context, _databaseName, subscriptionName);
                    if (subscriptionStateRaw == null)
                    {
                        subscriptionConnectionsState.DropSubscription(new SubscriptionDoesNotExistException($"The subscription {subscriptionName} had been deleted"));
                        continue;
                    }

                    var subscriptionState = JsonDeserializationClient.SubscriptionState(subscriptionStateRaw);
                    if (subscriptionState.Disabled)
                    {
                        subscriptionConnectionsState.DropSubscription(new SubscriptionClosedException($"The subscription {subscriptionName} is disabled and cannot be used until enabled"));
                        continue;
                    }

                    //make sure we only drop old connection and not new ones just arriving with the updated query
                    if (subscriptionConnectionsState != null && subscriptionState.Query != subscriptionConnectionsState.Query)
                    {
                        subscriptionConnectionsState.DropSubscription(new SubscriptionClosedException($"The subscription {subscriptionName} query has been modified, connection must be restarted", canReconnect: true));
                        continue;
                    }
                    
                    DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "check modifying of starting point");
                    /*
                    if (subscriptionState.LastClientConnectionTime == null && 
                        subscriptionState.ChangeVectorForNextBatchStartingPoint != subscriptionConnectionsState.LastChangeVectorSent)
                    {
                        subscriptionConnectionsState.DropSubscription(new SubscriptionClosedException($"The subscription {subscriptionName} was modified, connection must be restarted", canReconnect: true));
                        continue;
                    }
                    */

                    DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "create subscription WhosTaskIsIt");
                    DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Need to handle NodeTag, currently is isn't used for sharded because it is shared");

                    var whoseTaskIsIt = databaseRecord.TopologyForSubscriptions().WhoseTaskIsIt(_serverStore.Engine.CurrentState, subscriptionState);
                    if (whoseTaskIsIt != _serverStore.NodeTag)
                    {
                        subscriptionConnectionsState.DropSubscription(
                            new SubscriptionDoesNotBelongToNodeException("Subscription operation was stopped, because it's now under a different server's responsibility"));
                    }
                }
            }
        }

        public SubscriptionConnectionsStateOrchestrator GetSubscriptionConnectionsState<T>(TransactionOperationContext<T> context, string subscriptionName) where T : RavenTransaction
        {
            var subscriptionState = _serverStore.Cluster.Subscriptions.ReadSubscriptionStateByName(context, _databaseName, subscriptionName);

            if (SubscriptionsConnectionsState.TryGetValue(subscriptionState.SubscriptionId, out var concurrentSubscription) == false)
                return null;

            return concurrentSubscription;
        }

        public bool TryEnterSubscriptionsSemaphore() => ConcurrentConnectionsSemiSemaphore.Wait(0);

        public void ReleaseSubscriptionsSemaphore() => ConcurrentConnectionsSemiSemaphore.Release();
    }
}
