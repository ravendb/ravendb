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
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Subscriptions
{
    public abstract class SubscriptionStorageBase 
    {
        private readonly ServerStore _serverStore;

        protected readonly ConcurrentDictionary<long, SubscriptionConnectionState> _subscriptionConnectionStates = new ConcurrentDictionary<long, SubscriptionConnectionState>();

        protected SubscriptionStorageBase(ServerStore serverStore)
        {
            _serverStore = serverStore;
        }

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

        public async Task DeleteSubscription(string database, string name, string raftRequestId)
        {
            var command = new DeleteSubscriptionCommand(database, name, raftRequestId);
            var (etag, _) = await _serverStore.SendToLeaderAsync(command);
            await _serverStore.Cluster.WaitForIndexNotification(etag, _serverStore.Engine.OperationTimeout);
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
