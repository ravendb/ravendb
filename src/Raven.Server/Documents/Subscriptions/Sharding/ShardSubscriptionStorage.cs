using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Subscriptions.Sharding;

public sealed class ShardSubscriptionStorage : SubscriptionStorage
{
    private readonly string _shardName;
    private readonly int _shardNumber;

    public ShardSubscriptionStorage(ShardedDocumentDatabase db, ServerStore serverStore, string name) : base(db, serverStore, name)
    {
        _shardName = db.Name;
        _shardNumber = ShardHelper.GetShardNumberFromDatabaseName(_shardName);
    }

    public override void HandleDatabaseRecordChange()
    {
        using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            //checks which subscriptions should be dropped because of the database record change
            foreach ((long id, var state) in _subscriptions)
            {
                var subscriptionName = state.SubscriptionName;
                if (subscriptionName == null)
                    continue;

#pragma warning disable CS0618
                using var taskStateRaw = _serverStore.Cluster.Subscriptions.ReadSubscriptionStateRaw(context, _databaseName, subscriptionName);
#pragma warning restore CS0618
                if (taskStateRaw == null)
                {
                    // the subscription is deleted
                    continue;
                }

                if (state.IsSubscriptionActive() == false)
                    continue;

                var taskState = JsonDeserializationClient.SubscriptionState(taskStateRaw);

                if (SubscriptionChangeVectorHasChanges(state, taskState))
                {
                    // shard workers will be recreated on new orchestrator connection 
                    DropSubscriptionConnections(id, new SubscriptionChangeVectorUpdateConcurrencyException($"The subscription '{subscriptionName}' was modified on shard '{_shardName}', connection must be restarted"));
                    continue;
                }

                if (_serverStore.Engine.CurrentState == RachisState.Passive)
                {
                    DropSubscriptionConnections(id,
                        new SubscriptionDoesNotBelongToNodeException($"Subscription operation was stopped on '{_serverStore.NodeTag}', because current node state is '{RachisState.Passive}'."));
                }

                // we pass here RachisState.Follower so the task won't be disconnected if the node is in candidate state
                var whoseTaskIsIt = GetSubscriptionResponsibleNode(context, RachisState.Follower, taskState);
                if (whoseTaskIsIt != _serverStore.NodeTag)
                {
                    string reason = string.IsNullOrEmpty(whoseTaskIsIt) ? "could not get responsible node for subscription task." : $"because it's now under node '{whoseTaskIsIt}' responsibility.";

                    DropSubscriptionConnections(id,
                        new SubscriptionDoesNotBelongToNodeException($"Shard subscription '{id}' operation was stopped, {reason}"));
                    continue;
                }
            }
        }
    }

    protected override DatabaseTopology GetTopology(ClusterOperationContext context) => _serverStore.Cluster.ReadShardingConfiguration(context, _databaseName)?.Shards[_shardNumber];
    
    protected override string GetNodeFromState(SubscriptionState taskStatus)
    {
        taskStatus.ShardingState.NodeTagPerShard.TryGetValue(_shardName, out var tag);
        return tag;
    }

    protected override bool SubscriptionChangeVectorHasChanges(SubscriptionConnectionsState state, SubscriptionState taskStatus)
    {
        if (taskStatus.LastClientConnectionTime != null)
            return false;

        // the subscription connection is currently connecting
        if (state.SubscriptionState == null)
            return false;

        // persisted subscription state has different RaftCommandIndex than state of current connection
        // the subscription was modified and needs to reconnect
        return taskStatus.RaftCommandIndex != state.SubscriptionState.RaftCommandIndex;
    }

    internal override void CleanupSubscriptions()
    {
        // all shard workers are disposed by orchestrator directly
    }
}
