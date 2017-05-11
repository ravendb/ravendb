using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using Raven.Client.Documents.Exceptions.Subscriptions;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Util;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Client.Documents.Replication.Messages;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Json.Converters;
using Raven.Server.Rachis;

namespace Raven.Server.Documents.Subscriptions
{
    // todo: implement functionality for limiting amount of opened subscriptions
    public class SubscriptionStorage : IDisposable
    {
        private readonly DocumentDatabase _db;
        private readonly ServerStore _serverStore;
        public static TimeSpan TwoMinutesTimespan = TimeSpan.FromMinutes(2);
        private readonly ConcurrentDictionary<long, SubscriptionState> _subscriptionStates = new ConcurrentDictionary<long, SubscriptionState>();
        private readonly Logger _logger;

        public SubscriptionStorage(DocumentDatabase db, ServerStore serverStore)
        {
            _db = db;
            _serverStore = serverStore;
            _logger = LoggingSource.Instance.GetLogger<SubscriptionStorage>(db.Name);
        }

        public void Dispose()
        {
            foreach (var state in _subscriptionStates.Values)
            {
                state.Dispose();
            }            
        }

        public void Initialize()
        {
          
        }
        
        public async Task<long> CreateSubscription(SubscriptionCreationParams creationParams)
        {            
            var command = new CreateSubscriptionCommand(_db.Name)
            {
                Criteria = creationParams.Criteria,
                InitialChangeVector = creationParams.ChangeVector
            };
            
            var etag = await _serverStore.SendToLeaderAsync(command.ToJson());

            if (_logger.IsInfoEnabled)
                _logger.Info($"New Subscription With ID {etag} was created");

            await _serverStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, etag);
            //await _db.WaitForIndexNotification(etag);
            return etag;
        }

        public SubscriptionState OpenSubscription(SubscriptionConnection connection)
        {
            var subscriptionState = _subscriptionStates.GetOrAdd(connection.SubscriptionId,
                _ => new SubscriptionState(this));
            return subscriptionState;
        }

        public async Task AcknowledgeBatchProcessed(long id, ChangeVectorEntry[] changeVector)
        {
            var changeVectorForEtag = changeVector;                       

            var command = new AcknowledgeSubscriptionBatchCommand(_db.Name)
            {                
                ChangeVector = changeVectorForEtag,
                NodeTag = _serverStore.NodeTag,
                SubscriptionId = id
            };

            var etag = await _serverStore.SendToLeaderAsync(command.ToJson());
            await _serverStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, etag);
            //await _db.WaitForIndexNotification(etag);            
        }

        public (SubscriptionCriteria criteria, ChangeVectorEntry[] startChangeVector) GetCriteriaAndChangeVector(long id, DocumentsOperationContext context)
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
            using (serverStoreContext.OpenReadTransaction())
            {
                var subscription = GetSubscriptionFromServerStore(serverStoreContext, id);
                return (subscription.Criteria, subscription.ChangeVector);
            }            
        }

        public void AssertSubscriptionIdExists(long id, TimeSpan timeout)
        {
            Task.WaitAny(_serverStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, id), Task.Delay(timeout));

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
            using (serverStoreContext.OpenReadTransaction())
            {
                GetSubscriptionFromServerStore(serverStoreContext, id);
            }
        }

        public async Task DeleteSubscription(long id)
        {
            var command = new DeleteSubscriptionCommand(_db.Name)
            {
                SubscriptionId = id                
            };

            var etag = await _serverStore.SendToLeaderAsync(command.ToJson());

            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"Subscription with id {id} was deleted");
            }
            //await _db.WaitForIndexNotification(etag);           
            await _serverStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, etag);
        }

        public bool DropSubscriptionConnection(long subscriptionId, string reason)
        {
            SubscriptionState subscriptionState;
            if (_subscriptionStates.TryGetValue(subscriptionId, out subscriptionState) == false)
                return false;

            subscriptionState.Connection.ConnectionException = new SubscriptionClosedException(reason);
            subscriptionState.RegisterRejectedConnection(subscriptionState.Connection, new SubscriptionClosedException(reason));
            subscriptionState.Connection.CancellationTokenSource.Cancel();

            if (_logger.IsInfoEnabled)
                _logger.Info($"Subscription with id {subscriptionId} connection was dropped. Reason: {reason}");

            return true;
        }

        public IEnumerable<SubscriptionGeneralDataAndStats> GetAllSubscriptions(TransactionOperationContext serverStoreContext, bool history, int start, int take)
        {
            foreach (var subscriptionGeneralData in ClusterStateMachine.ReadValuesStartingWith(serverStoreContext,
                SubscriptionRaftState.GenerateSubscriptionPrefix(_db.Name)).Select(x=> new SubscriptionGeneralDataAndStats(JsonDeserializationClient.SubscriptionRaftState(x.Item2))))
            {
                GetSubscriptionInternal(subscriptionGeneralData, history);
                yield return subscriptionGeneralData;
            }
        }     

        private DynamicJsonValue GetSubscriptionConnectionStats(SubscriptionConnectionStats stats)
        {
            return new DynamicJsonValue
            {
                [nameof(SubscriptionConnectionStats.ConnectedAt)] = stats.ConnectedAt,
                [nameof(SubscriptionConnectionStats.LastMessageSentAt)] = stats.LastMessageSentAt,
                [nameof(SubscriptionConnectionStats.LastAckReceivedAt)] = stats.LastAckReceivedAt,


                [nameof(SubscriptionConnectionStats.DocsRate)] = stats.DocsRate.CreateMeterData(),
                [nameof(SubscriptionConnectionStats.BytesRate)] = stats.BytesRate.CreateMeterData(),
                [nameof(SubscriptionConnectionStats.AckRate)] = stats.AckRate.CreateMeterData()
            };
        }

        private DynamicJsonValue GetSubscriptionConnectionOptions(SubscriptionConnectionOptions options)
        {
            return new DynamicJsonValue
            {
                [nameof(SubscriptionConnectionOptions.SubscriptionId)] = options.SubscriptionId,
                [nameof(SubscriptionConnectionOptions.TimeToWaitBeforeConnectionRetryMilliseconds)] = options.TimeToWaitBeforeConnectionRetryMilliseconds,
                [nameof(SubscriptionConnectionOptions.IgnoreSubscriberErrors)] = options.IgnoreSubscriberErrors,
                [nameof(SubscriptionConnectionOptions.Strategy)] = options.Strategy,
                [nameof(SubscriptionConnectionOptions.MaxDocsPerBatch)] = options.MaxDocsPerBatch
            };
        }

        public IEnumerable<SubscriptionGeneralDataAndStats> GetAllRunningSubscriptions(TransactionOperationContext context, bool history, int start, int take)
        {

            foreach (var kvp in _subscriptionStates)
            {
                var subscriptionState = kvp.Value;
                var subscriptionId = kvp.Key;

                if (subscriptionState?.Connection == null)
                    continue;

                if (start > 0)
                {
                    start--;
                    continue;
                }

                if (take-- <= 0)
                    yield break;

                var subscriptionData = GetSubscriptionFromServerStore(context, subscriptionId);
                GetRunningSubscriptionInternal(history, subscriptionData, subscriptionState);
                yield return subscriptionData;
            }
        }

        public unsafe SubscriptionGeneralDataAndStats GetSubscription(TransactionOperationContext context, long id, bool history)
        {
            var subscription = GetSubscriptionFromServerStore(context, id);

            GetSubscriptionInternal(subscription, history);

            return subscription;
        }

        private SubscriptionGeneralDataAndStats GetSubscriptionFromServerStore(TransactionOperationContext context, long id)
        {
            var subscriptionBlittable = _serverStore.Cluster.Read(context, SubscriptionRaftState.GenerateSubscriptionItemName(_db.Name,id));

            if (subscriptionBlittable == null)
                throw new SubscriptionDoesNotExistException($"Subscripiton with id {id} was not found in server store");

            var subscriptionRaftState = JsonDeserializationClient.SubscriptionRaftState(subscriptionBlittable);
            var subscriptionJsonValue = new SubscriptionGeneralDataAndStats(subscriptionRaftState);
            return subscriptionJsonValue;
        }

        public SubscriptionGeneralDataAndStats GetRunningSubscription(TransactionOperationContext context, long id, bool history)
        {
            SubscriptionState subscriptionState;
            if (_subscriptionStates.TryGetValue(id, out subscriptionState) == false)
                return null;

            if (subscriptionState.Connection == null)
                return null;
            
            var subscriptionJsonValue = GetSubscriptionFromServerStore(context, id);
            GetRunningSubscriptionInternal(history, subscriptionJsonValue, subscriptionState);
            return subscriptionJsonValue;
        }
        public class SubscriptionGeneralDataAndStats: SubscriptionRaftState
        {
            public SubscriptionConnection Connection;
            public SubscriptionConnection[] RecentConnections;
            public SubscriptionConnection[] RecentRejectedConnections;

            public SubscriptionGeneralDataAndStats(){}

            public SubscriptionGeneralDataAndStats(SubscriptionRaftState @base)
            {
                Criteria = @base.Criteria;
                ChangeVector = @base.ChangeVector;
                SubscriptionId = @base.SubscriptionId;
            }
        }
        public SubscriptionGeneralDataAndStats GetRunningSubscriptionConnectionHistory(TransactionOperationContext context, long subscriptionId)
        {
            SubscriptionState subscriptionState;
            if (!_subscriptionStates.TryGetValue(subscriptionId, out subscriptionState))
                return null;

            var subscriptionConnection = subscriptionState.Connection;
            if (subscriptionConnection == null)
                return null;

            var databaseRecord = _serverStore.Cluster.ReadDatabase(context, _db.Name);

            var subscriptionData = GetSubscriptionFromServerStore(context, subscriptionId);
            subscriptionData.Connection = subscriptionState.Connection;
            SetSubscriptionHistory(subscriptionState, subscriptionData);

            return subscriptionData;            
        }

        public long GetRunningCount()
        {
            return _subscriptionStates.Count(x => x.Value.Connection != null);
        }
        
        public long GetAllSubscriptionsCount()
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return ClusterStateMachine.ReadValuesStartingWith(context, SubscriptionRaftState.GenerateSubscriptionPrefix(_db.Name))
                    .Count();
            }            
        }        

        private void SetSubscriptionHistory(SubscriptionState subscriptionState, SubscriptionGeneralDataAndStats subscriptionData)
        {            
            subscriptionData.RecentConnections = subscriptionState.RecentConnections;                                    
            subscriptionData.RecentRejectedConnections = subscriptionState.RecentRejectedConnections;
        }

        private void GetRunningSubscriptionInternal(bool history, SubscriptionGeneralDataAndStats subscriptionData, SubscriptionState subscriptionState)
        {
            subscriptionData.Connection = subscriptionState.Connection;
            if (history)
                SetSubscriptionHistory(subscriptionState, subscriptionData);            
        }

        private void GetSubscriptionInternal(SubscriptionGeneralDataAndStats subscriptionData, bool history)
        {
            SubscriptionState subscriptionState;
            if (_subscriptionStates.TryGetValue(subscriptionData.SubscriptionId, out subscriptionState))
            {
                subscriptionData.Connection = subscriptionState.Connection;                

                if (history)
                    SetSubscriptionHistory(subscriptionState, subscriptionData);
            }                        
        }

        public void HandleDatabaseRecordChange()
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var databaseRecord = _serverStore.Cluster.ReadDatabase(context, _db.Name);
                
                foreach (var subscripitonStateKvp in _subscriptionStates)
                {
                    var subscriptionBlittable = _serverStore.Cluster.Read(context, SubscriptionRaftState.GenerateSubscriptionItemName(_db.Name, subscripitonStateKvp.Key));
                    if (subscriptionBlittable== null)
                    {
                        DropSubscriptionConnection(subscripitonStateKvp.Key, "Deleted");
                        continue;
                    }
                    var subscriptionRaftState = JsonDeserializationClient.SubscriptionRaftState(subscriptionBlittable);
                    

                    if (databaseRecord.Topology.WhoseTaskIsIt(subscriptionRaftState) != _serverStore.NodeTag)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Disconnected subscripiton with id {subscripitonStateKvp.Key}, because it was is no longer managed by this node ({_serverStore.NodeTag})");
                        DropSubscriptionConnection(subscripitonStateKvp.Key, "Moved to another server");
                    }
                }
            }
        }
    }
}
 