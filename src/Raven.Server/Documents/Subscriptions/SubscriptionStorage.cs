// -----------------------------------------------------------------------
//  <copyright file="SubscriptionStorage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.LowMemory;

namespace Raven.Server.Documents.Subscriptions
{
    public class SubscriptionStorage : SubscriptionStorageBase, IDisposable, ILowMemoryHandler
    {
        private readonly DocumentDatabase _db;
        private readonly ServerStore _serverStore;
        private readonly Logger _logger;
        private readonly SemaphoreSlim _concurrentConnectionsSemiSemaphore;

        public event Action<string> OnAddTask;
        public event Action<string> OnRemoveTask;
        public event Action<SubscriptionConnection> OnEndConnection;
        public event Action<string, SubscriptionBatchStatsAggregator> OnEndBatch;

        public SubscriptionStorage(DocumentDatabase db, ServerStore serverStore) : base(serverStore)
        {
            _db = db;
            _serverStore = serverStore;
            _logger = LoggingSource.Instance.GetLogger<SubscriptionStorage>(db.Name);

            _concurrentConnectionsSemiSemaphore = new SemaphoreSlim(db.Configuration.Subscriptions.MaxNumberOfConcurrentConnections);
            LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
        }

        public void Dispose()
        {
            var aggregator = new ExceptionAggregator(_logger, "Error disposing SubscriptionStorage");
            foreach (var state in _subscriptionConnectionStates.Values)
            {
                aggregator.Execute(state.Dispose);
                aggregator.Execute(_concurrentConnectionsSemiSemaphore.Dispose);
            }
            aggregator.ThrowIfNeeded();
        }

        public void Initialize()
        {

        }

        public async Task<(long, long)> PutSubscription(SubscriptionCreationOptions options, string raftRequestId, long? subscriptionId = null, bool? disabled = false, string mentor = null)
        {
            var etag = await PutSubscription(_db.Name, options, raftRequestId, subscriptionId, disabled, mentor);

            if (subscriptionId != null)
            {
                // updated existing subscription
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Subscription '{options.Name}' with index '{etag}' was updated.");

                return (subscriptionId.Value, etag);
            }

            if (_logger.IsInfoEnabled)
                _logger.Info($"New Subscription '{options.Name}' with index '{etag}' was created.");

            _db.SubscriptionStorage.RaiseNotificationForTaskAdded(options.Name);

            return (etag, etag);
        }

        public SubscriptionConnectionState OpenSubscription(SubscriptionConnectionBase connection)
        {
            var subscriptionState = _subscriptionConnectionStates.GetOrAdd(connection.SubscriptionId,
                subscriptionId => new SubscriptionConnectionState(subscriptionId, connection.Options.SubscriptionName, this));
            return subscriptionState;
        }

        public async Task AcknowledgeBatchProcessed(string database, ShardData shardData, long id, string name, string changeVector, string previousChangeVector)
        {
            var command = new AcknowledgeSubscriptionBatchCommand(database, RaftIdGenerator.NewId())
            {
                ChangeVector = changeVector,
                NodeTag = _serverStore.NodeTag,
                HasHighlyAvailableTasks = _serverStore.LicenseManager.HasHighlyAvailableTasks(),
                SubscriptionId = id,
                SubscriptionName = name,
                LastTimeServerMadeProgressWithDocuments = DateTime.UtcNow,
                LastKnownSubscriptionChangeVector = previousChangeVector,
                ShardName = shardData.ShardName,
                ShardDbId = shardData.DatabaseId,
                ShardLocalChangeVector = shardData.LocalChangeVector,
            };

            var (etag, _) = await _serverStore.SendToLeaderAsync(command);
            await _db.RachisLogIndexNotifications.WaitForIndexNotification(etag, _serverStore.Engine.OperationTimeout);
        }

        public async Task UpdateClientConnectionTime(long id, string name, string database, ShardData shardData, string mentorNode = null)
        {
            var command = new UpdateSubscriptionClientConnectionTime(database, RaftIdGenerator.NewId())
            {
                NodeTag = _serverStore.NodeTag,
                HasHighlyAvailableTasks = _serverStore.LicenseManager.HasHighlyAvailableTasks(),
                SubscriptionName = name,
                LastClientConnectionTime = DateTime.UtcNow,
                ShardName = shardData.ShardName
            };

            var (etag, _) = await _serverStore.SendToLeaderAsync(command);
            await _db.RachisLogIndexNotifications.WaitForIndexNotification(etag, _serverStore.Engine.OperationTimeout);
        }

        public SubscriptionState GetSubscriptionFromServerStore(string name)
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
            using (serverStoreContext.OpenReadTransaction())
            {
                return GetSubscriptionFromServerStore(serverStoreContext, name);
            }
        }

        public SubscriptionState GetSubscriptionFromServerStoreById(long id)
        {
            return GetSubscriptionFromServerStoreById(_serverStore, _db.Name, id);
        }

        public string GetResponsibleNode(TransactionOperationContext serverContext, string name)
        {
            var subscription = GetSubscriptionFromServerStore(serverContext, name);
            var topology = _serverStore.Cluster.ReadDatabaseTopology(serverContext, _db.Name);
            return _db.WhoseTaskIsIt(topology, subscription, subscription);
        }

        public async Task<SubscriptionState> AssertSubscriptionConnectionDetails(long id, string name, string database, CancellationToken token)
        {
            await _serverStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, id, token);

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
            using (serverStoreContext.OpenReadTransaction())
            using (var record = _serverStore.Cluster.ReadRawDatabaseRecord(serverStoreContext, _db.Name))
            {
                var subscription = GetSubscriptionFromServerStore(_serverStore, serverStoreContext, database, name);
                var topology = record.Topology;

                var whoseTaskIsIt = _db.WhoseTaskIsIt(topology, subscription, subscription);
                if (whoseTaskIsIt == null && record.DeletionInProgress.ContainsKey(_serverStore.NodeTag))
                    throw new DatabaseDoesNotExistException($"Stopping subscription '{name}' on node {_serverStore.NodeTag}, because database '{_db.Name}' is being deleted.");

                if (whoseTaskIsIt != _serverStore.NodeTag)
                {
                    var databaseTopologyAvailabilityExplanation = new Dictionary<string, string>();

                    string generalState;
                    RachisState currentState = _serverStore.Engine.CurrentState;
                    if (currentState == RachisState.Candidate || currentState == RachisState.Passive)
                    {
                        generalState = $"Current node ({_serverStore.NodeTag}) is in {currentState.ToString()} state therefore, we can't answer who's task is it and returning null";
                    }
                    else
                    {
                        generalState = currentState.ToString();
                    }
                    databaseTopologyAvailabilityExplanation["NodeState"] = generalState;

                    FillNodesAvailabilityReportForState(subscription, topology, databaseTopologyAvailabilityExplanation, stateGroup: topology.Rehabs, stateName: "rehab");
                    FillNodesAvailabilityReportForState(subscription, topology, databaseTopologyAvailabilityExplanation, stateGroup: topology.Promotables, stateName: "promotable");

                    //whoseTaskIsIt!= null && whoseTaskIsIt == subscription.MentorNode 
                    foreach (var member in topology.Members)
                    {
                        if (whoseTaskIsIt != null)
                        {
                            if (whoseTaskIsIt == subscription.MentorNode && member == subscription.MentorNode)
                            {
                                databaseTopologyAvailabilityExplanation[member] = "Is the mentor node and a valid member of the topology, it should be the mentor node";
                            }
                            else if (whoseTaskIsIt != null && whoseTaskIsIt != member)
                            {
                                databaseTopologyAvailabilityExplanation[member] = "Is a valid member of the topology, but not chosen to be the node running the subscription";
                            }
                            else if (whoseTaskIsIt == member)
                            {
                                databaseTopologyAvailabilityExplanation[member] = "Is a valid member of the topology and is chosen to be running the subscription";
                            }
                        }
                        else
                        {
                            databaseTopologyAvailabilityExplanation[member] = "Is a valid member of the topology but was not chosen to run the subscription, we didn't find any other match either";
                        }
                    }
                    throw new SubscriptionDoesNotBelongToNodeException(
                        $"Subscription with id '{id}' and name '{name}' can't be processed on current node ({_serverStore.NodeTag}), because it belongs to {whoseTaskIsIt}",
                        whoseTaskIsIt,
                        databaseTopologyAvailabilityExplanation, id);
                }
                if (subscription.Disabled)
                    throw new SubscriptionClosedException($"The subscription with id '{id}' and name '{name}' is disabled and cannot be used until enabled");

                return subscription;
            }

            static void FillNodesAvailabilityReportForState(SubscriptionGeneralDataAndStats subscription, DatabaseTopology topology, Dictionary<string, string> databaseTopologyAvailabilityExplenation, List<string> stateGroup, string stateName)
            {
                foreach (var nodeInGroup in stateGroup)
                {
                    var rehabMessage = string.Empty;
                    if (subscription.MentorNode == nodeInGroup)
                    {
                        rehabMessage = $"Although this node is a mentor, it's state is {stateName} and can't run the subscription";
                    }
                    else
                    {
                        rehabMessage = $"Node's state is {stateName}, can't run subscription";
                    }

                    if (topology.DemotionReasons.TryGetValue(nodeInGroup, out var demotionReason))
                    {
                        rehabMessage = rehabMessage + ". Reason:" + demotionReason;
                    }

                    databaseTopologyAvailabilityExplenation[nodeInGroup] = rehabMessage;
                }
            }
        }

        public bool DropSubscriptionConnection(long subscriptionId, SubscriptionException ex)
        {
            if (_subscriptionConnectionStates.TryGetValue(subscriptionId, out SubscriptionConnectionState subscriptionConnectionState) == false)
                return false;

            var subscriptionConnection = subscriptionConnectionState.Connection;

            if (subscriptionConnection != null)
            {
                subscriptionConnectionState.RegisterRejectedConnection(subscriptionConnection, ex);
                subscriptionConnection.ConnectionException = ex;
                
                try
                {
                    subscriptionConnection.CancellationTokenSource.Cancel();
                }
                catch
                {
                    // ignored
                }
            }

            if (_logger.IsInfoEnabled)
                _logger.Info($"Subscription with id '{subscriptionId}' and name '{subscriptionConnectionState.SubscriptionName}' connection was dropped. Reason: {ex.Message}");

            return true;
        }

        public string GetSubscriptionNameById(TransactionOperationContext serverStoreContext, long id)
        {
            return GetSubscriptionNameById(serverStoreContext, _db.Name, id);
        }

        public SubscriptionGeneralDataAndStats GetSubscriptionFromServerStore(TransactionOperationContext context, string name)
        {
            return GetSubscriptionFromServerStore(_serverStore, context, _db.Name, name);
        }

        public bool TryGetRunningSubscriptionConnection(long subscriptionId, out SubscriptionConnectionBase connection)
        {
            connection = null;

            if (_subscriptionConnectionStates.TryGetValue(subscriptionId, out var state) == false)
                return false;

            var stateConnection = state.Connection;
            if (stateConnection == null)
                return false;

            connection = stateConnection;

            return true;
        }

        public SubscriptionConnectionState GetSubscriptionConnection(TransactionOperationContext context, string subscriptionName)
        {
            var subscriptionBlittable = _serverStore.Cluster.Read(context, SubscriptionState.GenerateSubscriptionItemKeyName(_db.Name, subscriptionName));
            if (subscriptionBlittable == null)
                return null;

            var subscriptionState = JsonDeserializationClient.SubscriptionState(subscriptionBlittable);

            if (_subscriptionConnectionStates.TryGetValue(subscriptionState.SubscriptionId, out SubscriptionConnectionState subscriptionConnection) == false)
                return null;

            return subscriptionConnection;
        }

        public long GetRunningCount()
        {
            return _subscriptionConnectionStates.Count(x => x.Value.Connection != null);
        }

        public long GetAllSubscriptionsCount()
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return ClusterStateMachine.ReadValuesStartingWith(context, SubscriptionState.SubscriptionPrefix(_db.Name))
                    .Count();
            }
        }

        public void HandleDatabaseRecordChange(DatabaseRecord databaseRecord)
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var subscriptionStateKvp in _subscriptionConnectionStates)
                {
                    var subscriptionName = subscriptionStateKvp.Value.Connection?.Options?.SubscriptionName;
                    if (subscriptionName == null)
                        continue;

                    var database = ShardHelper.IsShardedName(databaseRecord.DatabaseName) ? ShardHelper.ToDatabaseName(databaseRecord.DatabaseName) : databaseRecord.DatabaseName;
                    using var subscriptionBlittable = _serverStore.Cluster.Read(context, SubscriptionState.GenerateSubscriptionItemKeyName(database, subscriptionName));
                    if (subscriptionBlittable == null)
                    {
                        DropSubscriptionConnection(subscriptionStateKvp.Key, new SubscriptionDoesNotExistException($"The subscription {subscriptionName} had been deleted"));
                        continue;
                    }

                    var subscriptionState = JsonDeserializationClient.SubscriptionState(subscriptionBlittable);
                    if (subscriptionState.Disabled)
                    {
                        DropSubscriptionConnection(subscriptionStateKvp.Key, new SubscriptionClosedException($"The subscription {subscriptionName} is disabled and cannot be used until enabled"));
                        continue;
                    }

                    SubscriptionConnectionBase connection = subscriptionStateKvp.Value.Connection;
                    if (connection != null && subscriptionState.Query != connection.SubscriptionState.Query)
                    {
                        DropSubscriptionConnection(subscriptionStateKvp.Key, new SubscriptionClosedException($"The subscription {subscriptionName} query has been modified, connection must be restarted", canReconnect: true));
                        continue;
                    }

                    var whoseTaskIsIt = _db.WhoseTaskIsIt(databaseRecord.Topology, subscriptionState, subscriptionState);
                    if (whoseTaskIsIt != _serverStore.NodeTag)
                    {
                        DropSubscriptionConnection(subscriptionStateKvp.Key,
                            new SubscriptionDoesNotBelongToNodeException("Subscription operation was stopped, because it's now under different server's responsibility"));
                    }
                }
            }
        }

        public Task GetSubscriptionConnectionInUseAwaiter(long subscriptionId)
        {
            if (_subscriptionConnectionStates.TryGetValue(subscriptionId, out SubscriptionConnectionState state) == false)
                return Task.CompletedTask;

            return state.ConnectionInUse.WaitAsync();
        }

        public bool TryEnterSemaphore()
        {
            return _concurrentConnectionsSemiSemaphore.Wait(0);
        }

        public void ReleaseSubscriptionsSemaphore()
        {
            _concurrentConnectionsSemiSemaphore.Release();
        }

        internal void CleanupSubscriptions()
        {
            var maxTaskLifeTime = _db.Is32Bits ? TimeSpan.FromHours(12) : TimeSpan.FromDays(2);
            var oldestPossibleIdleSubscription = SystemTime.UtcNow - maxTaskLifeTime;

            foreach (var state in _subscriptionConnectionStates)
            {
                if (state.Value.Connection != null)
                    continue;

                var recentConnection = state.Value.MostRecentEndedConnection();

                if (recentConnection != null && recentConnection.Stats.LastMessageSentAt < oldestPossibleIdleSubscription)
                {
                    _subscriptionConnectionStates.Remove(state.Key, out _);
                }
            }
        }

        public void LowMemory(LowMemorySeverity lowMemorySeverity)
        {
            foreach (var state in _subscriptionConnectionStates)
            {
                if (state.Value.Connection != null)
                    continue;

                _subscriptionConnectionStates.Remove(state.Key, out _);
            }
        }

        public void LowMemoryOver()
        {
            // nothing to do here
        }
        
        public void RaiseNotificationForTaskAdded(string subscriptionName)
        {
            OnAddTask?.Invoke(subscriptionName);
        }
        
        public void RaiseNotificationForTaskRemoved(string subscriptionName)
        {
            OnRemoveTask?.Invoke(subscriptionName);
        }
        
        public void RaiseNotificationForConnectionEnded(SubscriptionConnection connection)
        {
            OnEndConnection?.Invoke(connection);
        }
        
        public void RaiseNotificationForBatchEnded(string subscriptionName, SubscriptionBatchStatsAggregator batchAggregator)
        {
            OnEndBatch?.Invoke(subscriptionName, batchAggregator);
        }
    }
}
