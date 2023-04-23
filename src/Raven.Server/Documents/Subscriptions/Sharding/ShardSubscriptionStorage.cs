using System.Diagnostics;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Subscriptions.Sharding;

public class ShardSubscriptionStorage : SubscriptionStorage
{
    private readonly string _shardName;
    private readonly int _shardNumber;

    public ShardSubscriptionStorage(ShardedDocumentDatabase db, ServerStore serverStore, string name) : base(db, serverStore, name)
    {
        _shardName = db.Name;
        _shardNumber = ShardHelper.GetShardNumberFromDatabaseName(_shardName);
    }

    public override void HandleDatabaseRecordChange(DatabaseRecord databaseRecord)
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

                var taskState = JsonDeserializationClient.SubscriptionState(taskStateRaw);

                if (SubscriptionChangeVectorHasChanges(state, taskState))
                {
                    DropSubscriptionConnections(id, new SubscriptionClosedException(
                        $"The subscription '{subscriptionName}' change vector was modified on shard '{_shardName}', connection must be restarted",
                        canReconnect: true));
                    continue;
                }
               
                var whoseTaskIsIt = GetSubscriptionResponsibleNode(context, taskState);
                if (whoseTaskIsIt != _serverStore.NodeTag)
                {
                    DropSubscriptionConnections(id,
                        new SubscriptionDoesNotBelongToNodeException($"Shard subscription '{id}' operation was stopped, because it's now under a different server's responsibility"));
                    continue;
                }
            }
        }
    }

    protected override DatabaseTopology GetTopology(ClusterOperationContext context) => _serverStore.Cluster.ReadShardingConfiguration(context, _databaseName).Shards[_shardNumber];
    
    protected override string GetNodeFromState(SubscriptionState taskStatus)
    {
        taskStatus.ShardingState.NodeTagPerShard.TryGetValue(_shardName, out var tag);
        return tag;
    }

    protected override bool SubscriptionChangeVectorHasChanges(SubscriptionConnectionsState state, SubscriptionState taskStatus)
    {
        if (taskStatus.LastClientConnectionTime != null)
            return false;

        Debug.Assert(taskStatus.ShardingState != null);

        if (taskStatus.ShardingState != null)
        {
            if (taskStatus.ShardingState.ChangeVectorForNextBatchStartingPointPerShard.TryGetValue(_shardName, out var cv) == false &&
                state.LastChangeVectorSent == null)
            {
                return false;
            }

            if (cv == state.LastChangeVectorSent)
                return false;
        }

        return true;
    }

    internal override void CleanupSubscriptions()
    {
        // all shard workers are disposed by orchestrator directly
    }
}
