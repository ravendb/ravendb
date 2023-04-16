using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.LowMemory;

namespace Raven.Server.Documents.Subscriptions;

public abstract class AbstractSubscriptionStorage<TState> : ILowMemoryHandler, IDisposable
    where TState : AbstractSubscriptionConnectionsState
{
    protected readonly ConcurrentDictionary<long, TState> _subscriptions = new();
    public ConcurrentDictionary<long, TState> Subscriptions => _subscriptions;
    protected readonly ServerStore _serverStore;
    protected string _databaseName;
    protected readonly SemaphoreSlim _concurrentConnectionsSemiSemaphore;
    protected Logger _logger;

    protected AbstractSubscriptionStorage(ServerStore serverStore, int maxNumberOfConcurrentConnections)
    {
        _serverStore = serverStore;
        _concurrentConnectionsSemiSemaphore = new SemaphoreSlim(maxNumberOfConcurrentConnections);
        LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
    }

    protected abstract void DropSubscriptionConnections(TState state, SubscriptionException ex);
    protected abstract void SetConnectionException(TState state, SubscriptionException ex);
    protected abstract string GetSubscriptionResponsibleNode(DatabaseRecord databaseRecord, SubscriptionState taskStatus);
    protected abstract bool SubscriptionChangeVectorHasChanges(TState state, SubscriptionState taskStatus);

    public IEnumerable<SubscriptionState> GetAllSubscriptionsFromServerStore(TransactionOperationContext context)
    {
        foreach (var state in SubscriptionsClusterStorage.GetAllSubscriptionsWithoutState(context, _databaseName, 0, int.MaxValue))
            yield return state;
    }

    public SubscriptionState GetSubscriptionById(TransactionOperationContext context, long taskId) => _serverStore.Cluster.Subscriptions.ReadSubscriptionStateById(context, _databaseName, taskId);

    public SubscriptionState GetSubscriptionByName(TransactionOperationContext context, string taskName) => _serverStore.Cluster.Subscriptions.ReadSubscriptionStateByName(context, _databaseName, taskName);

    public long GetAllSubscriptionsCount()
    {
        using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            return ClusterStateMachine.ReadValuesStartingWith(context, SubscriptionState.SubscriptionPrefix(_databaseName))
                .Count();
        }
    }

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

    public virtual void HandleDatabaseRecordChange(DatabaseRecord databaseRecord)
    {
        using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
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

                using var subscriptionStateRaw = _serverStore.Cluster.Subscriptions.ReadSubscriptionStateRaw(context, _databaseName, subscriptionName);
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
                    DropSubscriptionConnections(id, new SubscriptionClosedException($"The subscription {subscriptionName} query has been modified, connection must be restarted", canReconnect: true));
                    continue;
                }

                if (SubscriptionChangeVectorHasChanges(subscriptionConnectionsState, subscriptionState))
                {
                    DropSubscriptionConnections(id, new SubscriptionClosedException($"The subscription {subscriptionName} was modified, connection must be restarted", canReconnect: true));
                    continue;
                }

                var whoseTaskIsIt = GetSubscriptionResponsibleNode(databaseRecord, subscriptionState);
                if (whoseTaskIsIt != _serverStore.NodeTag)
                {
                    DropSubscriptionConnections(id,
                        new SubscriptionDoesNotBelongToNodeException("Subscription operation was stopped, because it's now under a different server's responsibility"));
                }
            }
        }
    }
    
    public string GetSubscriptionNameById(TransactionOperationContext serverStoreContext, long id)
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

    public TState GetSubscriptionConnectionsState<T>(TransactionOperationContext<T> context, string subscriptionName) where T : RavenTransaction
    {
        var subscriptionState = _serverStore.Cluster.Subscriptions.ReadSubscriptionStateByName(context, _databaseName, subscriptionName);

        if (_subscriptions.TryGetValue(subscriptionState.SubscriptionId, out TState concurrentSubscription) == false)
            return null;

        return concurrentSubscription;
    }

    public bool TryEnterSubscriptionsSemaphore()
    {
        return _concurrentConnectionsSemiSemaphore.Wait(0);
    }

    public void ReleaseSubscriptionsSemaphore()
    {
        _concurrentConnectionsSemiSemaphore.Release();
    }

    public abstract (OngoingTaskConnectionStatus ConnectionStatus, string ResponsibleNodeTag) GetSubscriptionConnectionStatusAndResponsibleNode(long subscriptionId, SubscriptionState state, DatabaseRecord databaseRecord);

    protected (OngoingTaskConnectionStatus ConnectionStatus, string ResponsibleNodeTag) GetSubscriptionConnectionStatusAndResponsibleNode(
        long subscriptionId,
        [NotNull] SubscriptionState state,
        [NotNull] DatabaseTopology topology)
    {
        if (state == null)
            throw new ArgumentNullException(nameof(state));
        if (topology == null)
            throw new ArgumentNullException(nameof(topology));
        if (subscriptionId <= 0)
            throw new ArgumentOutOfRangeException(nameof(subscriptionId));

        var tag = _serverStore.WhoseTaskIsIt(topology, state, state);
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
