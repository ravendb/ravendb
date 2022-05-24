// -----------------------------------------------------------------------
//  <copyright file="SubscriptionStorage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
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
using Sparrow.Binary;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Voron;

namespace Raven.Server.Documents.Subscriptions
{
    public class SubscriptionStorage : IDisposable, ILowMemoryHandler
    {
        internal readonly DocumentDatabase _db;
        private readonly ServerStore _serverStore;

        private readonly ConcurrentDictionary<long, SubscriptionConnectionsState> _subscriptions = new();
        private readonly Logger _logger;
        private readonly SemaphoreSlim _concurrentConnectionsSemiSemaphore;

        public event Action<string> OnAddTask;
        public event Action<string> OnRemoveTask;
        public event Action<SubscriptionConnection> OnEndConnection;
        public event Action<string, SubscriptionBatchStatsAggregator> OnEndBatch;

        public SubscriptionStorage(DocumentDatabase db, ServerStore serverStore)
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
            foreach (var state in _subscriptions.Values)
            {
                aggregator.Execute(state.Dispose);
                aggregator.Execute(_concurrentConnectionsSemiSemaphore.Dispose);
            }
            aggregator.ThrowIfNeeded();
        }

        public void Initialize()
        {

        }

        public async Task<long> PutSubscription(SubscriptionCreationOptions options, string raftRequestId, long? subscriptionId = null, bool? disabled = false, string mentor = null)
        {
            var command = new PutSubscriptionCommand(_db.Name, options.Query, mentor, raftRequestId)
            {
                InitialChangeVector = options.ChangeVector,
                SubscriptionName = options.Name,
                SubscriptionId = subscriptionId,
                Disabled = disabled ?? false
            };

            var (etag, _) = await _serverStore.SendToLeaderAsync(command);

            if (_logger.IsInfoEnabled)
                _logger.Info($"New Subscription with index {etag} was created");

            await _db.RachisLogIndexNotifications.WaitForIndexNotification(etag, _serverStore.Engine.OperationTimeout);

            if (subscriptionId != null)
            {
                // updated existing subscription
                return subscriptionId.Value;
            }

            _db.SubscriptionStorage.RaiseNotificationForTaskAdded(options.Name);

            return etag;
        }

        public SubscriptionConnectionsState OpenSubscription(SubscriptionConnection connection)
        {
            var subscriptionState = _subscriptions.GetOrAdd(connection.SubscriptionId, subId => new SubscriptionConnectionsState(subId, this));
            subscriptionState.Initialize(connection);
            return subscriptionState;
        }

        public async Task<long> RecordBatchRevisions(long subscriptionId, string subscriptionName, List<RevisionRecord> list, string previouslyRecordedChangeVector, string lastRecordedChangeVector)
        {
            var command = new RecordBatchSubscriptionDocumentsCommand(_db.Name, subscriptionId, subscriptionName, list, previouslyRecordedChangeVector, lastRecordedChangeVector, _serverStore.NodeTag, _serverStore.LicenseManager.HasHighlyAvailableTasks(), RaftIdGenerator.NewId());
            var (etag, _) = await _serverStore.SendToLeaderAsync(command);
            await _db.RachisLogIndexNotifications.WaitForIndexNotification(etag, _serverStore.Engine.OperationTimeout);
            return etag;
        }

        public async Task<long> RecordBatchDocuments(long subscriptionId, string subscriptionName, List<DocumentRecord> list, List<string> deleted,
            string previouslyRecordedChangeVector, string lastRecordedChangeVector)
        {
            var command = new RecordBatchSubscriptionDocumentsCommand(_db.Name, subscriptionId, subscriptionName, list, previouslyRecordedChangeVector, lastRecordedChangeVector, _serverStore.NodeTag, _serverStore.LicenseManager.HasHighlyAvailableTasks(), RaftIdGenerator.NewId());
            command.Deleted = deleted;
            var (etag, _) = await _serverStore.SendToLeaderAsync(command);
            await _db.RachisLogIndexNotifications.WaitForIndexNotification(etag, _serverStore.Engine.OperationTimeout);
            return etag;
        }

        public async Task LegacyAcknowledgeBatchProcessed(long subscriptionId, string name, string changeVector, string previousChangeVector)
        {
            var command = new AcknowledgeSubscriptionBatchCommand(_db.Name, RaftIdGenerator.NewId())
            {
                ChangeVector = changeVector,
                NodeTag = _serverStore.NodeTag,
                HasHighlyAvailableTasks = _serverStore.LicenseManager.HasHighlyAvailableTasks(),
                SubscriptionId = subscriptionId,
                SubscriptionName = name,
                LastTimeServerMadeProgressWithDocuments = DateTime.UtcNow,
                LastKnownSubscriptionChangeVector = previousChangeVector,
                DatabaseName = _db.Name,
            };

            var (etag, _) = await _serverStore.SendToLeaderAsync(command);
            await _db.RachisLogIndexNotifications.WaitForIndexNotification(etag, _serverStore.Engine.OperationTimeout);
        }

        public async Task AcknowledgeBatchProcessed(long subscriptionId, string name, string changeVector, long? batchId, List<DocumentRecord> docsToResend)
        {
            var command = new AcknowledgeSubscriptionBatchCommand(_db.Name, RaftIdGenerator.NewId())
            {
                ChangeVector = changeVector,
                NodeTag = _serverStore.NodeTag,
                HasHighlyAvailableTasks = _serverStore.LicenseManager.HasHighlyAvailableTasks(),
                SubscriptionId = subscriptionId,
                SubscriptionName = name,
                LastTimeServerMadeProgressWithDocuments = DateTime.UtcNow,
                BatchId = batchId,
                DatabaseName = _db.Name,
                DocumentsToResend = docsToResend
            };

            var (etag, _) = await _serverStore.SendToLeaderAsync(command);
            await _db.RachisLogIndexNotifications.WaitForIndexNotification(etag, _serverStore.Engine.OperationTimeout);
        }

        public async Task UpdateClientConnectionTime(long id, string name, string mentorNode = null)
        {
            var command = new UpdateSubscriptionClientConnectionTime(_db.Name, RaftIdGenerator.NewId())
            {
                NodeTag = _serverStore.NodeTag,
                HasHighlyAvailableTasks = _serverStore.LicenseManager.HasHighlyAvailableTasks(),
                SubscriptionName = name,
                LastClientConnectionTime = DateTime.UtcNow
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
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
            using (serverStoreContext.OpenReadTransaction())
            {
                var name = GetSubscriptionNameById(serverStoreContext, id);
                if (string.IsNullOrEmpty(name))
                    throw new SubscriptionDoesNotExistException($"Subscription with id '{id}' was not found in server store");

                return GetSubscriptionFromServerStore(serverStoreContext, name);
            }
        }

        public string GetResponsibleNode(TransactionOperationContext serverContext, string name)
        {
            var subscription = GetSubscriptionFromServerStore(serverContext, name);
            var topology = _serverStore.Cluster.ReadDatabaseTopology(serverContext, _db.Name);
            return _db.WhoseTaskIsIt(topology, subscription, subscription);
        }

        public async Task<SubscriptionState> AssertSubscriptionConnectionDetails(long id, string name, long? registerConnectionDurationInTicks, CancellationToken token)
        {
            await _serverStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, id, token);

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
            using (serverStoreContext.OpenReadTransaction())
            using (var record = _serverStore.Cluster.ReadRawDatabaseRecord(serverStoreContext, _db.Name))
            {
                var subscription = GetSubscriptionFromServerStore(serverStoreContext, name);
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
                        databaseTopologyAvailabilityExplanation, id)
                    {
                        RegisterConnectionDurationInTicks = registerConnectionDurationInTicks
                    };
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

        public async Task DeleteSubscription(string name, string raftRequestId)
        {
            var command = new DeleteSubscriptionCommand(_db.Name, name, raftRequestId);

            var (etag, _) = await _serverStore.SendToLeaderAsync(command);

            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"Subscription with name {name} was deleted");
            }
            await _db.RachisLogIndexNotifications.WaitForIndexNotification(etag, _serverStore.Engine.OperationTimeout);
        }

        public bool DropSubscriptionConnections(long subscriptionId, SubscriptionException ex)
        {
            if (_subscriptions.TryGetValue(subscriptionId, out SubscriptionConnectionsState subscriptionConnectionsState) == false)
                return false;

            foreach (var subscriptionConnection in subscriptionConnectionsState.GetConnections())
            {
                subscriptionConnectionsState.DropSingleConnection(subscriptionConnection, ex);
            }

            if (_logger.IsInfoEnabled)
                _logger.Info($"Subscription with id '{subscriptionId}' and name '{subscriptionConnectionsState.SubscriptionName}' connections were dropped.", ex);

            return true;
        }

        public bool DropSingleSubscriptionConnection(long subscriptionId, string workerId, SubscriptionException ex)
        {
            if (_subscriptions.TryGetValue(subscriptionId, out SubscriptionConnectionsState subscriptionConnectionsState) == false)
                return false;

            var connectionToDrop = subscriptionConnectionsState.GetConnections().FirstOrDefault(conn => conn.WorkerId == workerId);
            if (connectionToDrop == null)
                return false;

            subscriptionConnectionsState.DropSingleConnection(connectionToDrop, ex);
            if (_logger.IsInfoEnabled)
                _logger.Info(
                    $"Connection with id {workerId} in subscription with id '{subscriptionId}' and name '{subscriptionConnectionsState.SubscriptionName}' was dropped.", ex);

            return true;
        }

        public IEnumerable<SubscriptionGeneralDataAndStats> GetAllSubscriptions(TransactionOperationContext serverStoreContext, bool history, int start, int take)
        {
            foreach (var keyValue in ClusterStateMachine.ReadValuesStartingWith(serverStoreContext, SubscriptionState.SubscriptionPrefix(_db.Name)))
            {
                if (start > 0)
                {
                    start--;
                    continue;
                }

                if (take-- <= 0)
                    yield break;

                var subscriptionState = JsonDeserializationClient.SubscriptionState(keyValue.Value);
                var subscriptionConnectionsState = GetSubscriptionConnectionsState(serverStoreContext, subscriptionState.SubscriptionName);

                var subscriptionGeneralData = new SubscriptionGeneralDataAndStats(subscriptionState)
                {
                    Connections = subscriptionConnectionsState?.GetConnections(),
                    RecentConnections = subscriptionConnectionsState?.RecentConnections,
                    RecentRejectedConnections = subscriptionConnectionsState?.RecentRejectedConnections
                };
                GetSubscriptionInternal(subscriptionGeneralData, history);
                yield return subscriptionGeneralData;
            }
        }

        public string GetSubscriptionNameById(TransactionOperationContext serverStoreContext, long id)
        {
            foreach (var keyValue in ClusterStateMachine.ReadValuesStartingWith(serverStoreContext,
                SubscriptionState.SubscriptionPrefix(_db.Name)))
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

        public SubscriptionConnectionsState GetSubscriptionStateById(long id) => _subscriptions[id];

        public class ResendItem : IDynamicJson
        {
            public string Id;
            public long Batch;
            public string ChangeVector;
            public SubscriptionType Type;

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Id)] = Id,
                    [nameof(Batch)] = Batch,
                    [nameof(ChangeVector)] = ChangeVector,
                    [nameof(Type)] = Type.ToString()
                };
            }
        }

        public static IEnumerable<ResendItem> GetResendItems(ClusterOperationContext context, string database, long id)
        {
            var subscriptionState = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
            using (SubscriptionConnectionsState.GetDatabaseAndSubscriptionPrefix(context, database, id, out var prefix))
            using (Slice.External(context.Allocator, prefix, out var prefixSlice))
            {
                var resendItem = new ResendItem();

                foreach (var item in subscriptionState.SeekByPrimaryKeyPrefix(prefixSlice, Slices.Empty, 0))
                {
                    resendItem.Type = (SubscriptionType)item.Key[prefixSlice.Size];
                    resendItem.Id = item.Value.Reader.ReadStringWithPrefix((int)ClusterStateMachine.SubscriptionStateTable.Key, prefix.Length + 2);
                    resendItem.ChangeVector = item.Value.Reader.ReadString((int)ClusterStateMachine.SubscriptionStateTable.ChangeVector);
                    resendItem.Batch = Bits.SwapBytes(item.Value.Reader.ReadLong((int)ClusterStateMachine.SubscriptionStateTable.BatchId));

                    yield return resendItem;
                }
            }
        }

        public IEnumerable<SubscriptionGeneralDataAndStats> GetAllRunningSubscriptions(TransactionOperationContext context, bool history, int start, int take)
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

                var subscriptionData = GetSubscriptionFromServerStore(context, subscriptionConnectionsState.SubscriptionName);
                GetRunningSubscriptionInternal(history, subscriptionData, subscriptionConnectionsState);
                yield return subscriptionData;
            }
        }

        public int GetNumberOfRunningSubscriptions()
        {
            var c = 0;
            foreach ((_, SubscriptionConnectionsState value) in _subscriptions)
            {
                if (value.IsSubscriptionActive() == false)
                    continue;

                c++;
            }

            return c;
        }

        public SubscriptionGeneralDataAndStats GetSubscription(TransactionOperationContext context, long? id, string name, bool history)
        {
            SubscriptionGeneralDataAndStats subscription;

            if (string.IsNullOrEmpty(name) == false)
            {
                subscription = GetSubscriptionFromServerStore(context, name);
            }
            else if (id.HasValue)
            {
                subscription = GetSubscriptionFromServerStore(context, id.ToString());
            }
            else
            {
                throw new ArgumentNullException("Must receive either subscription id or subscription name in order to provide subscription data");
            }

            GetSubscriptionInternal(subscription, history);

            return subscription;
        }

        public SubscriptionGeneralDataAndStats GetSubscriptionFromServerStore(TransactionOperationContext context, string name)
        {
            var subscriptionBlittable = _serverStore.Cluster.Read(context, SubscriptionState.GenerateSubscriptionItemKeyName(_db.Name, name));

            if (subscriptionBlittable == null)
                throw new SubscriptionDoesNotExistException($"Subscription with name '{name}' was not found in server store");

            var subscriptionState = JsonDeserializationClient.SubscriptionState(subscriptionBlittable);
            var subscriptionConnectionsState = GetSubscriptionConnectionsState(context, subscriptionState.SubscriptionName);

            var subscriptionJsonValue = new SubscriptionGeneralDataAndStats(subscriptionState)
            {
                Connections = subscriptionConnectionsState?.GetConnections(),
                RecentConnections = subscriptionConnectionsState?.RecentConnections,
                RecentRejectedConnections = subscriptionConnectionsState?.RecentRejectedConnections
            };
            return subscriptionJsonValue;
        }

        public SubscriptionGeneralDataAndStats GetRunningSubscription(TransactionOperationContext context, long? id, string name, bool history)
        {
            SubscriptionGeneralDataAndStats subscription;
            if (string.IsNullOrEmpty(name) == false)
            {
                subscription = GetSubscriptionFromServerStore(context, name);
            }
            else if (id.HasValue)
            {
                name = GetSubscriptionNameById(context, id.Value);
                subscription = GetSubscriptionFromServerStore(context, name);
            }
            else
            {
                throw new ArgumentNullException("Must receive either subscription id or subscription name in order to provide subscription data");
            }

            if (_subscriptions.TryGetValue(subscription.SubscriptionId, out SubscriptionConnectionsState subscriptionConnectionsState) == false)
                return null;

            if (subscriptionConnectionsState.IsSubscriptionActive() == false)
                return null;

            GetRunningSubscriptionInternal(history, subscription, subscriptionConnectionsState);
            return subscription;
        }

        public bool TryGetRunningSubscriptionConnectionsState(long subscriptionId, out SubscriptionConnectionsState connections)
        {
            connections = null;

            if (_subscriptions.TryGetValue(subscriptionId, out var concurrentSubscription) == false)
                return false;

            if (concurrentSubscription == null)
                return false;

            connections = concurrentSubscription;

            return true;
        }

        public SubscriptionConnectionsState GetSubscriptionConnectionsState<T>(TransactionOperationContext<T> context, string subscriptionName) where T : RavenTransaction
        {
            var subscriptionBlittable = _serverStore.Cluster.Read(context, SubscriptionState.GenerateSubscriptionItemKeyName(_db.Name, subscriptionName));
            if (subscriptionBlittable == null)
                return null;

            var subscriptionState = JsonDeserializationClient.SubscriptionState(subscriptionBlittable);

            if (_subscriptions.TryGetValue(subscriptionState.SubscriptionId, out SubscriptionConnectionsState concurrentSubscription) == false)
                return null;

            return concurrentSubscription;
        }

        public class SubscriptionGeneralDataAndStats : SubscriptionState
        {
            public SubscriptionConnection Connection => Connections?.FirstOrDefault();
            public List<SubscriptionConnection> Connections;
            public IEnumerable<SubscriptionConnection> RecentConnections;
            public IEnumerable<SubscriptionConnection> RecentRejectedConnections;

            public SubscriptionGeneralDataAndStats() { }

            public SubscriptionGeneralDataAndStats(SubscriptionState @base)
            {
                Query = @base.Query;
                ChangeVectorForNextBatchStartingPoint = @base.ChangeVectorForNextBatchStartingPoint;
                SubscriptionId = @base.SubscriptionId;
                SubscriptionName = @base.SubscriptionName;
                MentorNode = @base.MentorNode;
                NodeTag = @base.NodeTag;
                LastBatchAckTime = @base.LastBatchAckTime;
                LastClientConnectionTime = @base.LastClientConnectionTime;
                Disabled = @base.Disabled;
            }
        }

        public long GetRunningCount()
        {
            return _subscriptions.Count(x => x.Value.IsSubscriptionActive());
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

        private static void SetSubscriptionHistory(SubscriptionConnectionsState subscriptionConnectionsState, SubscriptionGeneralDataAndStats subscriptionData)
        {
            subscriptionData.RecentConnections = subscriptionConnectionsState.RecentConnections;
            subscriptionData.RecentRejectedConnections = subscriptionConnectionsState.RecentRejectedConnections;
        }

        private static void GetRunningSubscriptionInternal(bool history, SubscriptionGeneralDataAndStats subscriptionData, SubscriptionConnectionsState subscriptionConnectionsState)
        {
            subscriptionData.Connections = subscriptionConnectionsState.GetConnections();
            if (history) // Only valid for this node
                SetSubscriptionHistory(subscriptionConnectionsState, subscriptionData);
        }

        private void GetSubscriptionInternal(SubscriptionGeneralDataAndStats subscriptionData, bool history)
        {
            if (_subscriptions.TryGetValue(subscriptionData.SubscriptionId, out SubscriptionConnectionsState concurrentSubscription))
            {
                subscriptionData.Connections = concurrentSubscription.GetConnections();

                if (history)//Only valid if this is my subscription
                    SetSubscriptionHistory(concurrentSubscription, subscriptionData);
            }
        }

        public void HandleDatabaseRecordChange(DatabaseRecord databaseRecord)
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

                    var subscriptionBlittable = _serverStore.Cluster.Read(context, SubscriptionState.GenerateSubscriptionItemKeyName(databaseRecord.DatabaseName, subscriptionName));
                    if (subscriptionBlittable == null)
                    {
                        DropSubscriptionConnections(subscriptionStateKvp.Key, new SubscriptionDoesNotExistException($"The subscription {subscriptionName} had been deleted"));
                        continue;
                    }

                    var subscriptionState = JsonDeserializationClient.SubscriptionState(subscriptionBlittable);
                    if (subscriptionState.Disabled)
                    {
                        DropSubscriptionConnections(subscriptionStateKvp.Key, new SubscriptionClosedException($"The subscription {subscriptionName} is disabled and cannot be used until enabled"));
                        continue;
                    }

                    var subscriptionConnectionsState = subscriptionStateKvp.Value;

                    //make sure we only drop old connection and not new ones just arriving with the updated query
                    if (subscriptionConnectionsState != null && subscriptionState.Query != subscriptionConnectionsState.Query)
                    {
                        DropSubscriptionConnections(subscriptionStateKvp.Key, new SubscriptionClosedException($"The subscription {subscriptionName} query has been modified, connection must be restarted", canReconnect: true));
                        continue;
                    }

                    if (subscriptionState.LastClientConnectionTime == null &&
                        subscriptionState.ChangeVectorForNextBatchStartingPoint != subscriptionConnectionsState.LastChangeVectorSent)
                    {
                        DropSubscriptionConnections(subscriptionStateKvp.Key, new SubscriptionClosedException($"The subscription {subscriptionName} was modified, connection must be restarted", canReconnect: true));
                        continue;
                    }

                    var whoseTaskIsIt = _db.WhoseTaskIsIt(databaseRecord.Topology, subscriptionState, subscriptionState);
                    if (whoseTaskIsIt != _serverStore.NodeTag)
                    {
                        DropSubscriptionConnections(subscriptionStateKvp.Key,
                            new SubscriptionDoesNotBelongToNodeException("Subscription operation was stopped, because it's now under a different server's responsibility"));
                    }
                }
            }
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

            foreach (var kvp in _subscriptions)
            {
                if (kvp.Value.IsSubscriptionActive())
                    continue;

                var recentConnection = kvp.Value.MostRecentEndedConnection();

                if (recentConnection != null && recentConnection.Stats.LastMessageSentAt < oldestPossibleIdleSubscription)
                {
                    if (_subscriptions.TryRemove(kvp.Key, out var subsState))
                    {
                        subsState.Dispose();
                    }
                }
            }
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
