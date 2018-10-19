using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;
using Raven.Server.ServerWide.Commands.Subscriptions;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Subscriptions
{

    public class SubscriptionStorage : IDisposable
    {
        private readonly DocumentDatabase _db;
        private readonly ServerStore _serverStore;
        public static TimeSpan TwoMinutesTimespan = TimeSpan.FromMinutes(2);
        public bool HasHighlyAvailableTasks;
        private readonly ConcurrentDictionary<long, SubscriptionConnectionState> _subscriptionConnectionStates = new ConcurrentDictionary<long, SubscriptionConnectionState>();
        private readonly Logger _logger;
        private readonly SemaphoreSlim _concurrentConnectionsSemiSemaphore;

        public SubscriptionStorage(DocumentDatabase db, ServerStore serverStore)
        {
            _db = db;
            _serverStore = serverStore;
            _logger = LoggingSource.Instance.GetLogger<SubscriptionStorage>(db.Name);

            _concurrentConnectionsSemiSemaphore = new SemaphoreSlim(db.Configuration.Subscriptions.MaxNumberOfConcurrentConnections);
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

        public async Task<long> PutSubscription(SubscriptionCreationOptions options, long? subscriptionId = null, bool? disabled = false, string mentor = null)
        {
            var command = new PutSubscriptionCommand(_db.Name, options.Query, mentor)
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
            return etag;
        }

        public SubscriptionConnectionState OpenSubscription(SubscriptionConnection connection)
        {
            var subscriptionState = _subscriptionConnectionStates.GetOrAdd(connection.SubscriptionId,
                subscriptionId => new SubscriptionConnectionState(subscriptionId, this));
            return subscriptionState;
        }

        public async Task AcknowledgeBatchProcessed(long id, string name, string changeVector, string previousChangeVector)
        {           
            var command = new AcknowledgeSubscriptionBatchCommand(_db.Name)
            {
                ChangeVector = changeVector,
                NodeTag = _serverStore.NodeTag,
                HasHighlyAvailableTasks = _serverStore.LicenseManager.HasHighlyAvailableTasks(),
                SubscriptionId = id,
                SubscriptionName = name,
                LastTimeServerMadeProgressWithDocuments = DateTime.UtcNow,
                LastKnownSubscriptionChangeVector = previousChangeVector
            };

            var (etag, _) = await _serverStore.SendToLeaderAsync(command);
            await _db.RachisLogIndexNotifications.WaitForIndexNotification(etag, _serverStore.Engine.OperationTimeout);
        }


        public async Task UpdateClientConnectionTime(long id, string name, string mentorNode = null)
        {
            var command = new UpdateSubscriptionClientConnectionTime(_db.Name)
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

        public async Task<SubscriptionState> AssertSubscriptionConnectionDetails(long id, string name)
        {
            await _serverStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, id);

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
            using (serverStoreContext.OpenReadTransaction())
            {
                var subscription = GetSubscriptionFromServerStore(serverStoreContext, name);
                var databaseRecord = _serverStore.Cluster.ReadDatabase(serverStoreContext, _db.Name, out var _);
                var whoseTaskIsIt = _db.WhoseTaskIsIt(databaseRecord.Topology, subscription, subscription);
                if (whoseTaskIsIt != _serverStore.NodeTag)
                {
                    throw new SubscriptionDoesNotBelongToNodeException($"Subscription with id {id} can't be processed on current node ({_serverStore.NodeTag}), because it belongs to {whoseTaskIsIt}")
                    {
                        AppropriateNode = whoseTaskIsIt
                    };
                }
                if (subscription.Disabled)
                    throw new SubscriptionClosedException($"The subscription {id} is disabled and cannot be used until enabled");

                return subscription;
            }
        }

        public async Task DeleteSubscription(string name)
        {
            var command = new DeleteSubscriptionCommand(_db.Name, name);

            var (etag, _) = await _serverStore.SendToLeaderAsync(command);

            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"Subscription with id {name} was deleted");
            }
            await _db.RachisLogIndexNotifications.WaitForIndexNotification(etag, _serverStore.Engine.OperationTimeout);
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
                _logger.Info($"Subscription with id {subscriptionId} connection was dropped. Reason: {ex.Message}");

            return true;
        }

        public IEnumerable<SubscriptionGeneralDataAndStats> GetAllSubscriptions(TransactionOperationContext serverStoreContext, bool history, int start, int take)
        {
            foreach (var keyValue in ClusterStateMachine.ReadValuesStartingWith(serverStoreContext,
                SubscriptionState.SubscriptionPrefix(_db.Name)))
            {
                var subscriptionState = JsonDeserializationClient.SubscriptionState(keyValue.Value);
                var subscriptionGeneralData = new SubscriptionGeneralDataAndStats(subscriptionState);
                GetSubscriptionInternal(subscriptionGeneralData, history);
                yield return subscriptionGeneralData;
            }
        }

        public IEnumerable<SubscriptionGeneralDataAndStats> GetAllRunningSubscriptions(TransactionOperationContext context, bool history, int start, int take)
        {

            foreach (var kvp in _subscriptionConnectionStates)
            {
                var subscriptionState = kvp.Value;

                var subscriptionStateConnection = subscriptionState.Connection;

                if (subscriptionStateConnection == null)
                    continue;

                if (start > 0)
                {
                    start--;
                    continue;
                }

                if (take-- <= 0)
                    yield break;
                
                var subscriptionData = GetSubscriptionFromServerStore(context, subscriptionStateConnection.Options.SubscriptionName);
                GetRunningSubscriptionInternal(history, subscriptionData, subscriptionState);
                yield return subscriptionData;
            }
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
                throw new SubscriptionDoesNotExistException($"Subscription with name {name} was not found in server store");

            var subscriptionState = JsonDeserializationClient.SubscriptionState(subscriptionBlittable);
            var subscriptionJsonValue = new SubscriptionGeneralDataAndStats(subscriptionState);
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
                subscription = GetSubscriptionFromServerStore(context, id.ToString());
            }
            else
            {
                throw new ArgumentNullException("Must receive either subscription id or subscription name in order to provide subscription data");
            }

            if (_subscriptionConnectionStates.TryGetValue(subscription.SubscriptionId, out SubscriptionConnectionState subscriptionConnectionState) == false)
                return null;

            if (subscriptionConnectionState.Connection == null)
                return null;
            
            GetRunningSubscriptionInternal(history, subscription, subscriptionConnectionState);
            return subscription;
        }

        public bool TryGetRunningSubscriptionConnection(long subscriptionId, out SubscriptionConnection connection)
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

        public class SubscriptionGeneralDataAndStats : SubscriptionState
        {
            public SubscriptionConnection Connection;
            public SubscriptionConnection[] RecentConnections;
            public SubscriptionConnection[] RecentRejectedConnections;

            public SubscriptionGeneralDataAndStats() { }

            public SubscriptionGeneralDataAndStats(SubscriptionState @base)
            {
                Query = @base.Query;
                ChangeVectorForNextBatchStartingPoint = @base.ChangeVectorForNextBatchStartingPoint;
                SubscriptionId = @base.SubscriptionId;
                LastBatchAckTime = @base.LastBatchAckTime;
                SubscriptionName = @base.SubscriptionName;
                MentorNode = @base.MentorNode;
            }
        }

        public SubscriptionGeneralDataAndStats GetRunningSubscriptionConnectionHistory(TransactionOperationContext context, long subscriptionId)
        {
            if (!_subscriptionConnectionStates.TryGetValue(subscriptionId, out SubscriptionConnectionState subscriptionConnectionState))
                return null;

            var subscriptionConnection = subscriptionConnectionState.Connection;
            if (subscriptionConnection == null)
                return null;

            var subscriptionData = GetSubscriptionFromServerStore(context, subscriptionConnection.Options.SubscriptionName);
            subscriptionData.Connection = subscriptionConnection;
            SetSubscriptionHistory(subscriptionConnectionState, subscriptionData);

            return subscriptionData;
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

        private static void SetSubscriptionHistory(SubscriptionConnectionState subscriptionConnectionState, SubscriptionGeneralDataAndStats subscriptionData)
        {
            subscriptionData.RecentConnections = subscriptionConnectionState.RecentConnections;
            subscriptionData.RecentRejectedConnections = subscriptionConnectionState.RecentRejectedConnections;
        }

        private static void GetRunningSubscriptionInternal(bool history, SubscriptionGeneralDataAndStats subscriptionData, SubscriptionConnectionState subscriptionConnectionState)
        {
            subscriptionData.Connection = subscriptionConnectionState.Connection;
            if (history) // Only valid for this node
                SetSubscriptionHistory(subscriptionConnectionState, subscriptionData);
        }

        private void GetSubscriptionInternal(SubscriptionGeneralDataAndStats subscriptionData, bool history)
        {
            if (_subscriptionConnectionStates.TryGetValue(subscriptionData.SubscriptionId, out SubscriptionConnectionState subscriptionConnectionState))
            {
                subscriptionData.Connection = subscriptionConnectionState.Connection;

                if (history)//Only valid if this is my subscription
                    SetSubscriptionHistory(subscriptionConnectionState, subscriptionData);
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

                    var subscriptionBlittable = _serverStore.Cluster.Read(context, SubscriptionState.GenerateSubscriptionItemKeyName(databaseRecord.DatabaseName, subscriptionName));
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

                    if (subscriptionState.Query != subscriptionStateKvp.Value.Connection?.SubscriptionState.Query)
                    {
                        DropSubscriptionConnection(subscriptionStateKvp.Key, new SubscriptionClosedException($"The subscription {subscriptionName} query has been modified, connection must be restarted"));
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
    }
}
