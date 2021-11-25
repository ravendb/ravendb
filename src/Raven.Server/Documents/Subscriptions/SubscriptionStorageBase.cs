// -----------------------------------------------------------------------
//  <copyright file="SubscriptionStorage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.LowMemory;

namespace Raven.Server.Documents.Subscriptions
{
    public abstract class SubscriptionStorageBase : IDisposable, ILowMemoryHandler
    {
        private readonly ServerStore _serverStore;
        protected readonly ConcurrentDictionary<long, SubscriptionConnectionState> _subscriptionConnectionStates = new ConcurrentDictionary<long, SubscriptionConnectionState>();
        protected readonly Logger _logger;
        public abstract string DatabaseName
        {
            get;
        }

        protected SubscriptionStorageBase(ServerStore serverStore, Logger logger)
        {
            _serverStore = serverStore;
            _logger = logger;
        }

        public abstract SubscriptionConnectionState OpenSubscription(SubscriptionConnectionBase connection);
        protected abstract void HandleWhoseTaskIsIt(DatabaseTopology topology, SubscriptionState subscriptionState, long subscriptionId);

        public async Task<long> PutSubscription(string databaseName, SubscriptionCreationOptions options, string raftRequestId, long? subscriptionId = null, bool? disabled = false, string mentor = null)
        {
            var command = new PutSubscriptionCommand(databaseName, options.Query, mentor, raftRequestId)
            {
                InitialChangeVector = options.ChangeVector,
                SubscriptionName = options.Name,
                SubscriptionId = subscriptionId,
                Disabled = disabled ?? false
            };

            var (etag, _) = await _serverStore.SendToLeaderAsync(command);

            await _serverStore.Cluster.WaitForIndexNotification(etag, _serverStore.Engine.OperationTimeout);

            return etag;
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

                    HandleWhoseTaskIsIt(databaseRecord.Topology, subscriptionState, subscriptionStateKvp.Key);
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

        public async Task DeleteSubscription(string name, string raftRequestId)
        {
            var command = new DeleteSubscriptionCommand(DatabaseName, name, raftRequestId);
            var (etag, _) = await _serverStore.SendToLeaderAsync(command);
            await _serverStore.Cluster.WaitForIndexNotification(etag, _serverStore.Engine.OperationTimeout);

            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"Subscription with name '{name}' was deleted in database '{DatabaseName}'.");
            }
        }

        public SubscriptionState GetSubscriptionFromServerStoreById(long id)
        {
            return GetSubscriptionFromServerStoreById(_serverStore, DatabaseName, id);
        }

        public static SubscriptionGeneralDataAndStats GetSubscriptionFromServerStore(ServerStore serverStore, TransactionOperationContext context, string databaseName, string name)
        {
            var subscriptionBlittable = serverStore.Cluster.Read(context, SubscriptionState.GenerateSubscriptionItemKeyName(databaseName, name));

            if (subscriptionBlittable == null)
                throw new SubscriptionDoesNotExistException($"Subscription with name '{name}' was not found in server store");

            var subscriptionState = JsonDeserializationClient.SubscriptionState(subscriptionBlittable);
            var subscriptionJsonValue = new SubscriptionGeneralDataAndStats(subscriptionState);
            return subscriptionJsonValue;
        }

        public SubscriptionConnectionState GetSubscriptionConnection(TransactionOperationContext context, string subscriptionName)
        {
            using var subscriptionBlittable = _serverStore.Cluster.Read(context, SubscriptionState.GenerateSubscriptionItemKeyName(DatabaseName, subscriptionName));
            if (subscriptionBlittable == null)
                return null;

            var subscriptionState = JsonDeserializationClient.SubscriptionState(subscriptionBlittable);

            if (_subscriptionConnectionStates.TryGetValue(subscriptionState.SubscriptionId, out SubscriptionConnectionState subscriptionConnection) == false)
                return null;
            return subscriptionConnection;
        }

        public IEnumerable<SubscriptionGeneralDataAndStats> GetAllSubscriptions(TransactionOperationContext serverStoreContext, string databaseName,  bool history, int start, int take)
        {
            foreach (var keyValue in ClusterStateMachine.ReadValuesStartingWith(serverStoreContext, SubscriptionState.SubscriptionPrefix(databaseName)))
            {
                if (start > 0)
                {
                    start--;
                    continue;
                }

                if (take-- <= 0)
                    yield break;

                var subscriptionState = JsonDeserializationClient.SubscriptionState(keyValue.Value);
                var subscriptionGeneralData = new SubscriptionGeneralDataAndStats(subscriptionState);
                GetSubscriptionInternal(subscriptionGeneralData, history);
                yield return subscriptionGeneralData;
            }
        }

        public IEnumerable<SubscriptionGeneralDataAndStats> GetAllRunningSubscriptions(TransactionOperationContext context, string databaseName, bool history, int start, int take)
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

                var subscriptionData = GetSubscriptionFromServerStore(_serverStore, context, databaseName, subscriptionStateConnection.Options.SubscriptionName);
                GetRunningSubscriptionInternal(history, subscriptionData, subscriptionState);
                yield return subscriptionData;
            }
        }

        public SubscriptionGeneralDataAndStats GetSubscription(TransactionOperationContext context, long? id, string databaseName, string name, bool history)
        {
            SubscriptionGeneralDataAndStats subscription;

            if (string.IsNullOrEmpty(name) == false)
            {
                subscription = GetSubscriptionFromServerStore(_serverStore, context, databaseName, name);
            }
            else if (id.HasValue)
            {
                subscription = GetSubscriptionFromServerStore(_serverStore, context, databaseName, id.ToString());
            }
            else
            {
                throw new ArgumentNullException("Must receive either subscription id or subscription name in order to provide subscription data");
            }


            GetSubscriptionInternal(subscription, history);

            return subscription;
        }

        public SubscriptionGeneralDataAndStats GetRunningSubscription(TransactionOperationContext context, long? id, string databaseName, string name, bool history)
        {
            SubscriptionGeneralDataAndStats subscription;
            if (string.IsNullOrEmpty(name) == false)
            {
                subscription = GetSubscriptionFromServerStore(_serverStore, context, databaseName, name);
            }
            else if (id.HasValue)
            {
                name = GetSubscriptionNameById(context, databaseName, id.Value);
                subscription = GetSubscriptionFromServerStore(_serverStore, context, databaseName, name);
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

        public static string GetSubscriptionNameById(TransactionOperationContext serverStoreContext, string databaseName, long id)
        {
            foreach (var keyValue in ClusterStateMachine.ReadValuesStartingWith(serverStoreContext,
                SubscriptionState.SubscriptionPrefix(databaseName)))
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

        private void GetSubscriptionInternal(SubscriptionGeneralDataAndStats subscriptionData, bool history)
        {
            if (_subscriptionConnectionStates.TryGetValue(subscriptionData.SubscriptionId, out SubscriptionConnectionState subscriptionConnectionState))
            {
                subscriptionData.Connection = subscriptionConnectionState.Connection;

                if (history)//Only valid if this is my subscription
                    SetSubscriptionHistory(subscriptionConnectionState, subscriptionData);
            }
        }

        private static void SetSubscriptionHistory(SubscriptionConnectionState subscriptionConnectionState, SubscriptionGeneralDataAndStats subscriptionData)
        {
            subscriptionData.RecentConnections = subscriptionConnectionState.RecentConnections;
            subscriptionData.RecentRejectedConnections = subscriptionConnectionState.RecentRejectedConnections;
        }

        internal static void GetRunningSubscriptionInternal(bool history, SubscriptionGeneralDataAndStats subscriptionData, SubscriptionConnectionState subscriptionConnectionState)
        {
            subscriptionData.Connection = subscriptionConnectionState.Connection;
            if (history) // Only valid for this node
                SetSubscriptionHistory(subscriptionConnectionState, subscriptionData);
        }

        public static SubscriptionState GetSubscriptionFromServerStoreById(ServerStore serverStore, string databaseName, long id)
        {
            using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
            using (serverStoreContext.OpenReadTransaction())
            {
                var name = GetSubscriptionNameById(serverStoreContext, databaseName, id);
                if (string.IsNullOrEmpty(name))
                    throw new SubscriptionDoesNotExistException($"Subscription with id '{id}' was not found in server store");

                return GetSubscriptionFromServerStore(serverStore, serverStoreContext, databaseName, name);
            }
        }

        internal void CleanupSubscriptions(bool is32Bits)
        {
            var maxTaskLifeTime = is32Bits ? TimeSpan.FromHours(12) : TimeSpan.FromDays(2);
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

        public void Dispose()
        {
            var aggregator = new ExceptionAggregator(_logger, "Error disposing SubscriptionStorage");
            foreach (var state in _subscriptionConnectionStates.Values)
            {
                aggregator.Execute(state.Dispose);
            }
            aggregator.ThrowIfNeeded();
        }
    }

    public class SubscriptionGeneralDataAndStats : SubscriptionState
    {
        public SubscriptionConnectionBase Connection;
        public IEnumerable<SubscriptionConnectionBase> RecentConnections;
        public IEnumerable<SubscriptionConnectionBase> RecentRejectedConnections;

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
}
