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
using Raven.Server.Utils;

namespace Raven.Server.Documents.Subscriptions
{
    // todo: implement functionality for limiting amount of opened subscriptions
    public class SubscriptionStorage : IDisposable
    {
        private readonly DocumentDatabase _db;
        private readonly ServerStore _serverStore;
        public static TimeSpan TwoMinutesTimespan = TimeSpan.FromMinutes(2);
        private readonly ConcurrentDictionary<long, SubscriptionConnectionState> _subscriptionStates = new ConcurrentDictionary<long, SubscriptionConnectionState>();
        private readonly Logger _logger;

        public SubscriptionStorage(DocumentDatabase db, ServerStore serverStore)
        {
            _db = db;
            _serverStore = serverStore;
            _logger = LoggingSource.Instance.GetLogger<SubscriptionStorage>(db.Name);
        }

        public void Dispose()
        {
            var aggregator = new ExceptionAggregator(_logger, "Error disposing subscripiton");
            foreach (var state in _subscriptionStates.Values)
            {
                aggregator.Execute(state.Dispose);                
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
            
            await _db.WaitForIndexNotification(etag);
            return etag;
        }

        public SubscriptionConnectionState OpenSubscription(SubscriptionConnection connection)
        {
            var subscriptionState = _subscriptionStates.GetOrAdd(connection.SubscriptionId,
                _ => new SubscriptionConnectionState(this));
            return subscriptionState;
        }

        public async Task AcknowledgeBatchProcessed(long id, long lastEtag, ChangeVectorEntry[] changeVector)
        {
            var changeVectorForEtag = changeVector;                       

            var command = new AcknowledgeSubscriptionBatchCommand(_db.Name)
            {                
                ChangeVector = changeVectorForEtag,
                NodeTag = _serverStore.NodeTag,
                SubscriptionId = id,
                LastEtagInDbId = lastEtag,
                DbId = _db.DbId
            };

            var etag = await _serverStore.SendToLeaderAsync(command.ToJson());
            await _db.WaitForIndexNotification(etag);            
        }

        public SubscriptionState GetSubscriptionFromServerStore(long id)
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
            using (serverStoreContext.OpenReadTransaction())
            {
                return GetSubscriptionFromServerStore(serverStoreContext, id);
            }            
        }

        public void AssertSubscriptionIdExists(long id, TimeSpan timeout)
        {
            _db.WaitForIndexNotification(id);

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
            await _db.WaitForIndexNotification(etag);
            
        }

        public bool DropSubscriptionConnection(long subscriptionId, string reason)
        {
            SubscriptionConnectionState subscriptionConnectionState;
            if (_subscriptionStates.TryGetValue(subscriptionId, out subscriptionConnectionState) == false)
                return false;

            subscriptionConnectionState.Connection.ConnectionException = new SubscriptionClosedException(reason);
            subscriptionConnectionState.RegisterRejectedConnection(subscriptionConnectionState.Connection, new SubscriptionClosedException(reason));
            subscriptionConnectionState.Connection.CancellationTokenSource.Cancel();

            if (_logger.IsInfoEnabled)
                _logger.Info($"Subscription with id {subscriptionId} connection was dropped. Reason: {reason}");

            return true;
        }

        public IEnumerable<SubscriptionGeneralDataAndStats> GetAllSubscriptions(TransactionOperationContext serverStoreContext, bool history, int start, int take)
        {
            foreach (var subscriptionStateBlittable in ClusterStateMachine.ReadValuesStartingWith(serverStoreContext,
                SubscriptionState.GenerateSubscriptionPrefix(_db.Name)))
            {
                var subscriptionState = JsonDeserializationClient.SubscriptionState(subscriptionStateBlittable.Item2);
                var subscriptionGeneralData = new SubscriptionGeneralDataAndStats(subscriptionState);
                GetSubscriptionInternal(subscriptionGeneralData, history);
                yield return subscriptionGeneralData;
            }
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

        public SubscriptionGeneralDataAndStats GetSubscription(TransactionOperationContext context, long id, bool history)
        {
            var subscription = GetSubscriptionFromServerStore(context, id);

            GetSubscriptionInternal(subscription, history);

            return subscription;
        }

        public SubscriptionGeneralDataAndStats GetSubscriptionFromServerStore(TransactionOperationContext context, long id)
        {
            var subscriptionBlittable = _serverStore.Cluster.Read(context, SubscriptionState.GenerateSubscriptionItemName(_db.Name,id));

            if (subscriptionBlittable == null)
                throw new SubscriptionDoesNotExistException($"Subscripiton with id {id} was not found in server store");

            var subscriptionState = JsonDeserializationClient.SubscriptionState(subscriptionBlittable);
            var subscriptionJsonValue = new SubscriptionGeneralDataAndStats(subscriptionState);
            return subscriptionJsonValue;
        }

        public SubscriptionGeneralDataAndStats GetRunningSubscription(TransactionOperationContext context, long id, bool history)
        {
            SubscriptionConnectionState subscriptionConnectionState;
            if (_subscriptionStates.TryGetValue(id, out subscriptionConnectionState) == false)
                return null;

            if (subscriptionConnectionState.Connection == null)
                return null;
            
            var subscriptionJsonValue = GetSubscriptionFromServerStore(context, id);
            GetRunningSubscriptionInternal(history, subscriptionJsonValue, subscriptionConnectionState);
            return subscriptionJsonValue;
        }
        public class SubscriptionGeneralDataAndStats: SubscriptionState
        {
            public SubscriptionConnection Connection;
            public SubscriptionConnection[] RecentConnections;
            public SubscriptionConnection[] RecentRejectedConnections;

            public SubscriptionGeneralDataAndStats(){}

            public SubscriptionGeneralDataAndStats(SubscriptionState @base)
            {
                Criteria = @base.Criteria;
                ChangeVector = @base.ChangeVector;
                SubscriptionId = @base.SubscriptionId;
            }
        }
        public SubscriptionGeneralDataAndStats GetRunningSubscriptionConnectionHistory(TransactionOperationContext context, long subscriptionId)
        {
            SubscriptionConnectionState subscriptionConnectionState;
            if (!_subscriptionStates.TryGetValue(subscriptionId, out subscriptionConnectionState))
                return null;

            var subscriptionConnection = subscriptionConnectionState.Connection;
            if (subscriptionConnection == null)
                return null;

            var databaseRecord = _serverStore.Cluster.ReadDatabase(context, _db.Name);

            var subscriptionData = GetSubscriptionFromServerStore(context, subscriptionId);
            subscriptionData.Connection = subscriptionConnectionState.Connection;
            SetSubscriptionHistory(subscriptionConnectionState, subscriptionData);

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
                return ClusterStateMachine.ReadValuesStartingWith(context, SubscriptionState.GenerateSubscriptionPrefix(_db.Name))
                    .Count();
            }            
        }        

        private void SetSubscriptionHistory(SubscriptionConnectionState subscriptionConnectionState, SubscriptionGeneralDataAndStats subscriptionData)
        {            
            subscriptionData.RecentConnections = subscriptionConnectionState.RecentConnections;                                    
            subscriptionData.RecentRejectedConnections = subscriptionConnectionState.RecentRejectedConnections;
        }

        private void GetRunningSubscriptionInternal(bool history, SubscriptionGeneralDataAndStats subscriptionData, SubscriptionConnectionState subscriptionConnectionState)
        {
            subscriptionData.Connection = subscriptionConnectionState.Connection;
            if (history)
                SetSubscriptionHistory(subscriptionConnectionState, subscriptionData);            
        }

        private void GetSubscriptionInternal(SubscriptionGeneralDataAndStats subscriptionData, bool history)
        {
            SubscriptionConnectionState subscriptionConnectionState;
            if (_subscriptionStates.TryGetValue(subscriptionData.SubscriptionId, out subscriptionConnectionState))
            {
                subscriptionData.Connection = subscriptionConnectionState.Connection;                

                if (history)
                    SetSubscriptionHistory(subscriptionConnectionState, subscriptionData);
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
                    var subscriptionBlittable = _serverStore.Cluster.Read(context, SubscriptionState.GenerateSubscriptionItemName(_db.Name, subscripitonStateKvp.Key));
                    if (subscriptionBlittable== null)
                    {
                        DropSubscriptionConnection(subscripitonStateKvp.Key, "Deleted");
                        continue;
                    }
                    var subscriptionState = JsonDeserializationClient.SubscriptionState(subscriptionBlittable);
                    

                    if (databaseRecord.Topology.WhoseTaskIsIt(subscriptionState) != _serverStore.NodeTag)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Disconnected subscripiton with id {subscripitonStateKvp.Key}, because it was is no longer managed by this node ({_serverStore.NodeTag})");
                        DropSubscriptionConnection(subscripitonStateKvp.Key, "Moved to another server");
                    }
                }
            }
        }

        public Task GetSusbscriptionConnectionInUseAwaiter(long subscriptionId)
        {
            if (_subscriptionStates.TryGetValue(subscriptionId, out SubscriptionConnectionState state) == false)
                return Task.CompletedTask;

            return state.ConnectionInUse.WaitAsync();
        }
    }
}
 