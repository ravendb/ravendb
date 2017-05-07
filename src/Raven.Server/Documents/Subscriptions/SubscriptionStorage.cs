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

            var etag = await _serverStore.SendToLeaderAsync(command);

            if (_logger.IsInfoEnabled)
                _logger.Info($"New Subscription With ID {etag} was created");

            await _db.WaitForIndexNotification(etag);
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
                SubscriptionEtag = id
            };

            var etag = await _serverStore.SendToLeaderAsync(command);

            await _db.WaitForIndexNotification(etag);            
        }

        public (SubscriptionCriteria criteria, ChangeVectorEntry[] startChangeVector) GetCriteriaAndChangeVector(long id, DocumentsOperationContext context)
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
            using (serverStoreContext.OpenReadTransaction())
            {
                var databaseRecord = _serverStore.Cluster.ReadDatabase(serverStoreContext, _db.Name);
                if (databaseRecord.Subscriptions.TryGetValue(id.ToString(), out SubscriptionRaftState subscriptionRaftState) == false)
                {
                    throw new SubscriptionDoesNotExistException(
                            "There is no subscription configuration for specified identifier (id: " + id + ")");
                }                
                var criteria = subscriptionRaftState.Criteria;
                var startChangeVector = subscriptionRaftState.ChangeVector;

                return (criteria, startChangeVector);
            }            
        }

        public void AssertSubscriptionIdExists(long id, TimeSpan timeout)
        {
            Task.WaitAny(_db.WaitForIndexNotification(id), Task.Delay(timeout));
            
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
            using (serverStoreContext.OpenReadTransaction())
            {
                var databaseRecord = _serverStore.Cluster.ReadDatabase(serverStoreContext, _db.Name);

                if (databaseRecord.Subscriptions.ContainsKey(id.ToString()) == false)
                    throw new SubscriptionDoesNotExistException(
                            "There is no subscription configuration for specified identifier (id: " + id + ")");
            }
        }

        public async Task DeleteSubscription(long id)
        {
            var command = new DeleteSubscriptionCommand(_db.Name)
            {
                SubscriptionEtag = id                
            };

            var etag = await _serverStore.SendToLeaderAsync(command);

            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"Subscription with id {id} was deleted");
            }
            await _db.WaitForIndexNotification(etag);           
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
            var databaseRecord = _serverStore.Cluster.ReadDatabase(serverStoreContext, _db.Name);
                        
            foreach (var subscriptionGeneralData in databaseRecord.Subscriptions.Values.Select(x=> new SubscriptionGeneralDataAndStats
            {
                General = x
            }))
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
            var databaseRecord = _serverStore.Cluster.ReadDatabase(context, _db.Name);
                
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
                
                var subscriptionData = GetSubscriptionGeneralData(databaseRecord, subscriptionId);
                GetRunningSubscriptionInternal(history, subscriptionData, subscriptionState);
                yield return subscriptionData;
            }

        }     

        public SubscriptionGeneralDataAndStats GetSubscriptionGeneralData(DatabaseRecord databaseRecord, long id)
        {
            if (databaseRecord.Subscriptions.TryGetValue(id.ToString(), out SubscriptionRaftState subscriptionInDatabaseRecord) == false)
                throw new SubscriptionDoesNotExistException(
                        "There is no subscription configuration for specified identifier (id: " + id + ")");

            return new SubscriptionGeneralDataAndStats
            {
                General = subscriptionInDatabaseRecord
            };
        }

        public unsafe SubscriptionGeneralDataAndStats GetSubscription(TransactionOperationContext context, long id, bool history)
        {
            var databaseRecord = _serverStore.Cluster.ReadDatabase(context, _db.Name);

            var subscriptionJsonValue = GetSubscriptionGeneralData(databaseRecord, id);

            GetSubscriptionInternal(subscriptionJsonValue, history);

            return subscriptionJsonValue;
        }

        public SubscriptionGeneralDataAndStats GetRunningSubscription(TransactionOperationContext context, long id, bool history)
        {
            SubscriptionState subscriptionState;
            if (_subscriptionStates.TryGetValue(id, out subscriptionState) == false)
                return null;

            if (subscriptionState.Connection == null)
                return null;

            var databaseRecord = _serverStore.Cluster.ReadDatabase(context, _db.Name);

            var subscriptionJsonValue = GetSubscriptionGeneralData(databaseRecord, id);
            GetRunningSubscriptionInternal(history, subscriptionJsonValue, subscriptionState);
            return subscriptionJsonValue;
        }
        public class SubscriptionGeneralDataAndStats
        {
            public SubscriptionRaftState General;
            public SubscriptionConnection Connection;
            public SubscriptionConnection[] RecentConnections;
            public SubscriptionConnection[] RecentRejectedConnections;
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

            var subscriptionData = GetSubscriptionGeneralData(databaseRecord, subscriptionId);
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
            {
                var databaseRecord = _serverStore.Cluster.Read(context, _db.Name);
                databaseRecord.TryGet(nameof(DatabaseRecord.Subscriptions), out BlittableJsonReaderArray subscriptionsReader);
                return subscriptionsReader.Length;
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
            if (_subscriptionStates.TryGetValue(subscriptionData.General.SubscriptionId, out subscriptionState))
            {
                subscriptionData.Connection = subscriptionState.Connection;                

                if (history)
                    SetSubscriptionHistory(subscriptionState, subscriptionData);
            }                        
        }       
    }
}
 