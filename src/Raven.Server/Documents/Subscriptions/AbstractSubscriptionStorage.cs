using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.DataArchival;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.LowMemory;
using static Raven.Server.Documents.Subscriptions.SubscriptionStorage;

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
    public abstract SubscriptionState GetRunningSubscription(ClusterOperationContext context, long? id, string name, bool history);

    public abstract bool DisableSubscriptionTasks { get; }
    private readonly TimeSpan _waitForClusterStabilizationTimeout;

    protected AbstractSubscriptionStorage(ServerStore serverStore, int maxNumberOfConcurrentConnections)
    {
        _serverStore = serverStore;
        _concurrentConnectionsSemiSemaphore = new SemaphoreSlim(maxNumberOfConcurrentConnections);
        _waitForClusterStabilizationTimeout = TimeSpan.FromMilliseconds(Math.Max(30000, (int)(2 * serverStore.Engine.OperationTimeout.TotalMilliseconds)));
    }

    public bool ShouldWaitForClusterStabilization()
    {
        var lastState = _serverStore.Engine.LastState;
        if (lastState == null)
            return false;

        switch (lastState.To)
        {
            // get last cluster state
            case RachisState.Passive:
                // if the last state was passive, we will throw on next cluster command
                return false;
            case RachisState.Candidate:
            {
                if (DateTime.UtcNow - lastState.When < _waitForClusterStabilizationTimeout)
                {
                    return true;
                }

                return false;
            }
            default:
                // we are fine to proceed with the subscription on this node
                return false;
        }
    }

    public string GetSubscriptionResponsibleNode(ClusterOperationContext context, SubscriptionState taskStatus)
    {
        return GetSubscriptionResponsibleNode(context, _serverStore.Engine.CurrentState, taskStatus);
    }

    internal string GetSubscriptionResponsibleNode(ClusterOperationContext context, RachisState currentState, SubscriptionState taskStatus)
    {
        var topology = GetTopology(context);
        var tag = GetNodeFromState(taskStatus);
        var lastFunc = UpdateValueForDatabaseCommand.GetLastResponsibleNode(_serverStore.LicenseManager.HasHighlyAvailableTasks(), topology, tag);
        return topology.WhoseTaskIsIt(currentState, taskStatus, lastFunc);
    }

    public static string GetSubscriptionResponsibleNodeForProgress(RawDatabaseRecord record, string shardName, SubscriptionState taskStatus, bool hasHighlyAvailableTasks)
    {
        var topology = string.IsNullOrEmpty(shardName) ? record.Topology : record.Sharding.Shards[ShardHelper.GetShardNumberFromDatabaseName(shardName)];
        var tag = string.IsNullOrEmpty(shardName) ? taskStatus.NodeTag : taskStatus.ShardingState.NodeTagPerShard.GetOrDefault(shardName);
        var lastResponsibleNode = UpdateValueForDatabaseCommand.GetLastResponsibleNode(hasHighlyAvailableTasks, topology, tag);
        return topology.WhoseTaskIsIt(RachisState.Follower, taskStatus, lastResponsibleNode);
    }

    public static string GetSubscriptionResponsibleNodeForConnection(RawDatabaseRecord record, SubscriptionState taskStatus, bool hasHighlyAvailableTasks)
    {
        var topology = record.TopologyForSubscriptions();
        var lastResponsibleNode = UpdateValueForDatabaseCommand.GetLastResponsibleNode(hasHighlyAvailableTasks, topology, taskStatus.NodeTag);
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
    
    public abstract ArchivedDataProcessingBehavior GetDefaultArchivedDataProcessingBehavior();
    
    public SubscriptionState GetSubscriptionByName(ClusterOperationContext context, string taskName)
    {
#pragma warning disable CS0618
        var state = _serverStore.Cluster.Subscriptions.ReadSubscriptionStateByName(context, _databaseName, taskName);
#pragma warning restore CS0618

        if (state.ArchivedDataProcessingBehavior is null)
        {
            // from 5.x version
            state.ArchivedDataProcessingBehavior = GetDefaultArchivedDataProcessingBehavior();
        }
        return state;
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

    public virtual SubscriptionState GetSubscriptionFromServerStore(ClusterOperationContext context, string name)
    {
        var subscriptionBlittable = _serverStore.Cluster.Read(context, SubscriptionState.GenerateSubscriptionItemKeyName(_databaseName, name));

        if (subscriptionBlittable == null)
            throw new SubscriptionDoesNotExistException($"Subscription with name '{name}' was not found in server store");

        var subscriptionState = JsonDeserializationClient.SubscriptionState(subscriptionBlittable);

        return subscriptionState;
    }

    public bool TryEnterSubscriptionsSemaphore()
    {
        return _concurrentConnectionsSemiSemaphore.Wait(0);
    }

    public void ReleaseSubscriptionsSemaphore()
    {
        try
        {
            _concurrentConnectionsSemiSemaphore.Release();
        }
        catch (ObjectDisposedException)
        {
            // Do nothing
        }
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

    public int GetAllRunningSubscriptionsCount()
    {
        var count = 0;
        foreach (var kvp in _subscriptions)
        {
            var subscriptionConnectionsState = kvp.Value;

            if (subscriptionConnectionsState.IsSubscriptionActive() == false)
                continue;

            count++;
        }
        return count;
    }

    public virtual void HandleDatabaseRecordChange()
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

                if (subscriptionStateKvp.Value.IsSubscriptionActive() == false)
                    continue;

                SubscriptionState subscriptionState = JsonDeserializationClient.SubscriptionState(subscriptionStateRaw);
                if (subscriptionState.Disabled || DisableSubscriptionTasks)
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

                if (_serverStore.Engine.CurrentState == RachisState.Passive)
                {
                    DropSubscriptionConnections(id,
                        new SubscriptionDoesNotBelongToNodeException($"Subscription operation was stopped on '{_serverStore.NodeTag}', because current node state is '{RachisState.Passive}'."));
                }

                var whoseTaskIsIt = GetSubscriptionResponsibleNode(context, RachisState.Follower, subscriptionState);
                if (whoseTaskIsIt != _serverStore.NodeTag)
                {
                    var reason = string.IsNullOrEmpty(whoseTaskIsIt) ? "could not get responsible node for subscription task." : $"because it's now under node '{whoseTaskIsIt}' responsibility.";
                    DropSubscriptionConnections(id,
                        new SubscriptionDoesNotBelongToNodeException($"Subscription operation was stopped, {reason}"));
                }
            }
        }
    }

    public TState GetSubscriptionConnectionsState(ClusterOperationContext context, string subscriptionName)
    {
        var subscriptionBlittable = _serverStore.Cluster.Read(context, SubscriptionState.GenerateSubscriptionItemKeyName(_databaseName, subscriptionName));
        if (subscriptionBlittable == null)
            return null;

        if (subscriptionBlittable.TryGet(nameof(SubscriptionState.SubscriptionId), out long id) == false)
            return null;

        if (_subscriptions.TryGetValue(id, out TState concurrentSubscription) == false)
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

    protected static void SetSubscriptionHistory(AbstractSubscriptionConnectionsState subscriptionConnectionsState, SubscriptionDataAbstractBase subscriptionData)
    {
        subscriptionData.RecentConnections = subscriptionConnectionsState.RecentConnections;
        subscriptionData.RecentRejectedConnections = subscriptionConnectionsState.RecentRejectedConnections;
        subscriptionData.CurrentPendingConnections = subscriptionConnectionsState.PendingConnections;
    }

    protected SubscriptionState GetRunningSubscriptionInternal(ClusterOperationContext context, long? id, string name, bool history, out TState connectionsState)
    {
        SubscriptionState state;
        if (string.IsNullOrEmpty(name) == false)
        {
            state = GetSubscriptionFromServerStore(context, name);
        }
        else if (id.HasValue)
        {
            name = GetSubscriptionNameById(context, id.Value);
            state = GetSubscriptionFromServerStore(context, name);
        }
        else
        {
            throw new ArgumentNullException("Must receive either subscription id or subscription name in order to provide subscription data");
        }

        if (_subscriptions.TryGetValue(state.SubscriptionId, out connectionsState) == false)
            return null;

        if (connectionsState.IsSubscriptionActive() == false)
            return null;

        return state;
    }

    public virtual SubscriptionState GetSubscription(ClusterOperationContext context, long? id, string name, bool history)
    {
        SubscriptionState state;

        if (string.IsNullOrEmpty(name) == false)
        {
            state = GetSubscriptionFromServerStore(context, name);
        }
        else if (id.HasValue)
        {
            state = GetSubscriptionFromServerStore(context, id.ToString());
        }
        else
        {
            throw new ArgumentNullException("Must receive either subscription id or subscription name in order to provide subscription data");
        }

        return state;
    }

    public IEnumerable<(SubscriptionState, TState)> GetAllRunningSubscriptionsInternal(ClusterOperationContext context, bool history, int start, int take)
    {
        foreach (var kvp in _subscriptions)
        {
            var subscriptionConnectionsState = kvp.Value;

            if (subscriptionConnectionsState.IsSubscriptionActive() == false)
                continue;

            if (start > 0)
            {
                start--;
                continue;
            }

            if (take-- <= 0)
                yield break;

            var state = GetSubscriptionFromServerStore(context, subscriptionConnectionsState.SubscriptionName);

            yield return (state, subscriptionConnectionsState);
        }
    }

    public virtual IEnumerable<SubscriptionState> GetAllSubscriptions(ClusterOperationContext context, bool history, int start, int take)
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

            var task = JsonDeserializationClient.SubscriptionState(keyValue.Value);

            yield return task;
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

    public abstract class SubscriptionDataAbstractBase : SubscriptionState
    {
        public IEnumerable<SubscriptionConnectionInfo> RecentConnections;
        public IEnumerable<SubscriptionConnectionInfo> RecentRejectedConnections;
        public IEnumerable<SubscriptionConnectionInfo> CurrentPendingConnections;
    }

    public class SubscriptionDataBase<T> : SubscriptionDataAbstractBase
    {
        public List<T> Connections;

        public SubscriptionDataBase() { }

        public SubscriptionDataBase(SubscriptionState @base)
        {
            Query = @base.Query;
            ChangeVectorForNextBatchStartingPoint = @base.ChangeVectorForNextBatchStartingPoint;
            SubscriptionId = @base.SubscriptionId;
            SubscriptionName = @base.SubscriptionName;
            ArchivedDataProcessingBehavior = @base.ArchivedDataProcessingBehavior;
            MentorNode = @base.MentorNode;
            PinToMentorNode = @base.PinToMentorNode;
            NodeTag = @base.NodeTag;
            LastBatchAckTime = @base.LastBatchAckTime;
            LastClientConnectionTime = @base.LastClientConnectionTime;
            Disabled = @base.Disabled;
            ShardingState = @base.ShardingState;
            RaftCommandIndex = @base.RaftCommandIndex;
        }
    }
}
