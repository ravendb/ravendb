using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Server.Utils;
using Sparrow.Server;
using Voron;

namespace Raven.Server.Documents.Subscriptions;

public abstract class AbstractSubscriptionStorage
{
    protected readonly ServerStore _serverStore;
    protected string _databaseName;
    protected readonly SemaphoreSlim _concurrentConnectionsSemiSemaphore;
    protected Logger _logger;

    protected abstract string GetNodeFromState(SubscriptionState taskStatus);
    protected abstract DatabaseTopology GetTopology(ClusterOperationContext context);
    public abstract bool DropSingleSubscriptionConnection(long subscriptionId, string workerId, SubscriptionException ex);

    protected AbstractSubscriptionStorage(ServerStore serverStore, int maxNumberOfConcurrentConnections)
    {
        _serverStore = serverStore;
        _concurrentConnectionsSemiSemaphore = new SemaphoreSlim(maxNumberOfConcurrentConnections);
    }

    public string GetSubscriptionResponsibleNode(ClusterOperationContext context, SubscriptionState taskStatus)
    {
        var topology = GetTopology(context);
        var tag = GetNodeFromState(taskStatus);
        var lastFunc = GetLastResponsibleNode(_serverStore.LicenseManager.HasHighlyAvailableTasks(), topology, tag);
        return topology.WhoseTaskIsIt(_serverStore.Engine.CurrentState, taskStatus, lastFunc);
    }

    public static string GetSubscriptionResponsibleNodeForProgress(RawDatabaseRecord record, string shardName, SubscriptionState taskStatus, bool hasHighlyAvailableTasks)
    {
        var topology = string.IsNullOrEmpty(shardName) ? record.Topology : record.Sharding.Shards[ShardHelper.GetShardNumberFromDatabaseName(shardName)];
        var tag = string.IsNullOrEmpty(shardName) ? taskStatus.NodeTag : taskStatus.ShardingState.NodeTagPerShard.GetOrDefault(shardName);
        var lastResponsibleNode = GetLastResponsibleNode(hasHighlyAvailableTasks, topology, tag);
        return topology.WhoseTaskIsIt(RachisState.Follower, taskStatus, lastResponsibleNode);
    }

    public static string GetSubscriptionResponsibleNodeForConnection(RawDatabaseRecord record, SubscriptionState taskStatus, bool hasHighlyAvailableTasks)
    {
        var topology = record.TopologyForSubscriptions();
        var lastResponsibleNode = GetLastResponsibleNode(hasHighlyAvailableTasks, topology, taskStatus.NodeTag);
        return topology.WhoseTaskIsIt(RachisState.Follower, taskStatus, lastResponsibleNode);
    }

    public IEnumerable<SubscriptionState> GetAllSubscriptionsFromServerStore(ClusterOperationContext context, int start = 0, int take = int.MaxValue)
    {
        foreach (var keyValue in ClusterStateMachine.ReadValuesStartingWith(context, SubscriptionState.SubscriptionPrefix(_databaseName)))
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

    public SubscriptionState GetSubscriptionById(ClusterOperationContext context, long subscriptionId)
    {
        var name = GetSubscriptionNameById(context, subscriptionId);
        if (string.IsNullOrEmpty(name))
            throw new SubscriptionDoesNotExistException($"Subscription with id '{subscriptionId}' was not found in server store");

        return GetSubscriptionByName(context, name);
    }

    public SubscriptionState GetSubscriptionByName(ClusterOperationContext context, string taskName)
    {
#pragma warning disable CS0618
        return _serverStore.Cluster.Subscriptions.ReadSubscriptionStateByName(context, _databaseName, taskName);
#pragma warning restore CS0618
    }

    
    public static IEnumerable<ResendItem> GetResendItemsForDatabase(ClusterOperationContext context, string database)
    {
        var subscriptionState = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
        using var _ = Slice.From(context.Allocator, database.ToLowerInvariant(), SpecialChars.RecordSeparator, ByteStringType.Immutable, out var dbNamePrefix);
        {
            foreach (var item in subscriptionState.SeekByPrimaryKeyPrefix(dbNamePrefix, Slices.Empty, 0))
            {
                yield return new ResendItem
                {
                    Type = (SubscriptionType)item.Key[dbNamePrefix.Size + sizeof(long) + sizeof(byte)],
                    Id = item.Value.Reader.ReadStringWithPrefix((int)ClusterStateMachine.SubscriptionStateTable.Key,
                        dbNamePrefix.Size + sizeof(long) + sizeof(byte) + sizeof(byte) + sizeof(byte)),
                    ChangeVector = item.Value.Reader.ReadString((int)ClusterStateMachine.SubscriptionStateTable.ChangeVector),
                    Batch = Bits.SwapBytes(item.Value.Reader.ReadLong((int)ClusterStateMachine.SubscriptionStateTable.BatchId)),
                    SubscriptionId = item.Value.Reader.ReadLongWithPrefix((int)ClusterStateMachine.SubscriptionStateTable.Key, dbNamePrefix.Size)
                };
            }
        }
    }

    public long GetAllSubscriptionsCount()
    {
        using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            return ClusterStateMachine.ReadValuesStartingWith(context, SubscriptionState.SubscriptionPrefix(_databaseName))
                .Count();
        }
    }
 
    public string GetSubscriptionNameById(ClusterOperationContext serverStoreContext, long id)
    {
        foreach (var keyValue in ClusterStateMachine.ReadValuesStartingWith(serverStoreContext,
                     SubscriptionState.SubscriptionPrefix(_databaseName)))
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

    public bool TryEnterSubscriptionsSemaphore()
    {
        return _concurrentConnectionsSemiSemaphore.Wait(0);
    }

    public void ReleaseSubscriptionsSemaphore()
    {
        _concurrentConnectionsSemiSemaphore.Release();
    }

    private static Func<string> GetLastResponsibleNode(
        bool hasHighlyAvailableTasks,
        DatabaseTopology topology,
        string nodeTag)
    {
        return () =>
        {
            if (hasHighlyAvailableTasks)
                return null;

            if (topology.Members.Contains(nodeTag) == false)
                return null;

            return nodeTag;
        };
    }
}

public abstract class AbstractSubscriptionStorage<TState> : AbstractSubscriptionStorage, ILowMemoryHandler, IDisposable
    where TState : AbstractSubscriptionConnectionsState
{
    protected readonly ConcurrentDictionary<long, TState> _subscriptions = new();
    public ConcurrentDictionary<long, TState> Subscriptions => _subscriptions;

    protected AbstractSubscriptionStorage(ServerStore serverStore, int maxNumberOfConcurrentConnections) : base(serverStore, maxNumberOfConcurrentConnections)
    {
        LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
    }

    protected abstract void DropSubscriptionConnections(TState state, SubscriptionException ex);
    protected abstract void SetConnectionException(TState state, SubscriptionException ex);
    protected abstract bool SubscriptionChangeVectorHasChanges(TState state, SubscriptionState taskStatus);

    public bool DropSubscriptionConnections(long subscriptionId, SubscriptionException ex)
    {
        if (_subscriptions.TryGetValue(subscriptionId, out TState state) == false)
            return false;

        DropSubscriptionConnections(state, ex);

        if (_logger.IsInfoEnabled)
            _logger.Info($"Subscription with id '{subscriptionId}' and name '{state.SubscriptionName}' connections were dropped.", ex);

        return true;
    }

    private bool DeleteAndSetException(long subscriptionId, SubscriptionException ex)
    {
        if (_subscriptions.TryRemove(subscriptionId, out TState state) == false)
            return false;

        SetConnectionException(state, ex);

        state.Dispose();

        if (_logger.IsInfoEnabled)
            _logger.Info($"Subscription with id '{subscriptionId}' and name '{state.SubscriptionName}' was deleted and connections were dropped.", ex);

        return true;
    }

    public SubscriptionState GetActiveSubscription(ClusterOperationContext context, long? id, string name)
    {
        SubscriptionState subscription;
        if (string.IsNullOrEmpty(name) == false)
        {
            subscription = GetSubscriptionByName(context, name);
        }
        else if (id.HasValue)
        {
            name = GetSubscriptionNameById(context, id.Value);
            subscription = GetSubscriptionByName(context, name);
        }
        else
        {
            throw new ArgumentNullException("Must receive either subscription id or subscription name in order to provide subscription data");
        }

        if (_subscriptions.TryGetValue(subscription.SubscriptionId, out TState subscriptionConnectionsState) == false)
            return null;

        if (subscriptionConnectionsState.IsSubscriptionActive() == false)
            return null;

        return subscription;
    }

    public virtual void HandleDatabaseRecordChange(DatabaseRecord databaseRecord)
    {
        using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            //checks which subscriptions should be dropped because of the database record change
            foreach (var subscriptionStateKvp in _subscriptions)
            {
                var subscriptionName = subscriptionStateKvp.Value.SubscriptionName;
                if (subscriptionName == null)
                    continue;

                var id = subscriptionStateKvp.Key;
                var subscriptionConnectionsState = subscriptionStateKvp.Value;

#pragma warning disable CS0618
                using var subscriptionStateRaw = _serverStore.Cluster.Subscriptions.ReadSubscriptionStateRaw(context, _databaseName, subscriptionName);
#pragma warning restore CS0618
                if (subscriptionStateRaw == null)
                {
                    DeleteAndSetException(id, new SubscriptionDoesNotExistException($"The subscription {subscriptionName} had been deleted"));
                    continue;
                }

                SubscriptionState subscriptionState = JsonDeserializationClient.SubscriptionState(subscriptionStateRaw);
                if (subscriptionState.Disabled)
                {
                    DropSubscriptionConnections(id, new SubscriptionClosedException($"The subscription {subscriptionName} is disabled and cannot be used until enabled"));
                    continue;
                }


                //make sure we only drop old connection and not new ones just arriving with the updated query
                if (subscriptionConnectionsState != null && subscriptionState.Query != subscriptionConnectionsState.Query)
                {
                    DropSubscriptionConnections(id,
                        new SubscriptionClosedException($"The subscription {subscriptionName} query has been modified, connection must be restarted",
                            canReconnect: true));
                    continue;
                }

                if (SubscriptionChangeVectorHasChanges(subscriptionConnectionsState, subscriptionState))
                {
                    DropSubscriptionConnections(id,
                        new SubscriptionClosedException($"The subscription {subscriptionName} was modified, connection must be restarted", canReconnect: true));
                    continue;
                }

                var whoseTaskIsIt = GetSubscriptionResponsibleNode(context, subscriptionState);
                if (whoseTaskIsIt != _serverStore.NodeTag)
                {
                    DropSubscriptionConnections(id,
                        new SubscriptionDoesNotBelongToNodeException("Subscription operation was stopped, because it's now under a different server's responsibility"));
                }
            }
        }
    }

    public TState GetSubscriptionConnectionsState(ClusterOperationContext context, string subscriptionName)
    {
        var subscriptionState = GetSubscriptionByName(context, subscriptionName);

        if (_subscriptions.TryGetValue(subscriptionState.SubscriptionId, out TState concurrentSubscription) == false)
            return null;

        return concurrentSubscription;
    }

    public (OngoingTaskConnectionStatus ConnectionStatus, string ResponsibleNodeTag) GetSubscriptionConnectionStatusAndResponsibleNode(
        ClusterOperationContext context,
        long subscriptionId,
        SubscriptionState state)
    {
        if (state == null)
            throw new ArgumentNullException(nameof(state));
        if (subscriptionId <= 0)
            throw new ArgumentOutOfRangeException(nameof(subscriptionId));

        var tag = GetSubscriptionResponsibleNode(context, state);
        OngoingTaskConnectionStatus connectionStatus = OngoingTaskConnectionStatus.NotActive;
        if (tag != _serverStore.NodeTag)
        {
            connectionStatus = OngoingTaskConnectionStatus.NotOnThisNode;
        }
        else if (TryGetRunningSubscriptionConnectionsState(subscriptionId, out var connectionsState))
        {
            connectionStatus = connectionsState.IsSubscriptionActive() ? OngoingTaskConnectionStatus.Active : OngoingTaskConnectionStatus.NotActive;
        }

        return (connectionStatus, tag);
    }

    public bool TryGetRunningSubscriptionConnectionsState(long subscriptionId, out TState connections)
    {
        return _subscriptions.TryGetValue(subscriptionId, out connections) && connections != null;
    }

    public void LowMemory(LowMemorySeverity lowMemorySeverity)
    {
        foreach (var state in _subscriptions)
        {
            if (state.Value.IsSubscriptionActive())
                continue;

            if (_subscriptions.TryRemove(state.Key, out var subsState))
            {
                subsState.Dispose();
            }
        }
    }

    public void LowMemoryOver()
    {
        // nothing to do here
    }

    public void Dispose()
    {
        var aggregator = new ExceptionAggregator(_logger, $"Error disposing '{nameof(AbstractSubscriptionStorage<TState>)}<{nameof(TState)}>'");
        foreach (var state in _subscriptions.Values)
        {
            aggregator.Execute(state.Dispose);
            aggregator.Execute(_concurrentConnectionsSemiSemaphore.Dispose);
        }

        aggregator.ThrowIfNeeded();
    }
}
