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

        // todo: maybe switch etag in subscriptions to ChangeVector...
        public async Task<long> CreateSubscription(BlittableJsonReaderObject criteria, long ackEtag = 0)
        {
            // Validate that this can be properly parsed into a criteria object
            // and doing that without holding the tx lock
            var criteriaInstance = JsonDeserializationServer.SubscriptionCriteria(criteria);
            
            var command = new CreateSubscriptionCommand(_db.Name)
            {
                Criteria = criteriaInstance,
                InitialChangeVector = GetChangeVectorFromEtag(ackEtag)
            };

            var etag = await _serverStore.SendToLeaderAsync(command);

            if (_logger.IsInfoEnabled)
                _logger.Info($"New Subscription With ID {etag} was created");

            await _db.WaitForIndexNotification(etag);
            return etag;
        }

        private ChangeVectorEntry[] GetChangeVectorFromEtag(long ackEtag)
        {
            ChangeVectorEntry[] changeVectorForEtag = null;

            if (ackEtag != 0)
            {
                using (_db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var docEqualsOrBiggerThenEtag = _db.DocumentsStorage.GetDocumentsFrom(context, ackEtag, 0, 1).First();
                    changeVectorForEtag = docEqualsOrBiggerThenEtag.ChangeVector;
                }
            }

            return changeVectorForEtag;
        }

        public SubscriptionState OpenSubscription(SubscriptionConnection connection)
        {
            var subscriptionState = _subscriptionStates.GetOrAdd(connection.SubscriptionId,
                _ => new SubscriptionState(this));
            return subscriptionState;
        }

        public async Task AcknowledgeBatchProcessed(long id, long lastEtag)
        {
            var changeVectorForEtag = GetChangeVectorFromEtag(lastEtag);                       

            var command = new AcknowledgeSubscriptionBatchCommand(_db.Name)
            {                
                ChangeVector = changeVectorForEtag,
                NodeTag = _serverStore.NodeTag,
                SubscriptionEtag = id
            };

            var etag = await _serverStore.SendToLeaderAsync(command);

            await _db.WaitForIndexNotification(etag);            
        }

        public unsafe void GetCriteriaAndEtag(long id, DocumentsOperationContext context, out SubscriptionCriteria criteria, out ChangeVectorEntry[] startChangeVector)
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
            using (serverStoreContext.OpenReadTransaction())
            {
                var databaseRecord = _serverStore.Cluster.ReadDatabase(serverStoreContext, _db.Name);
                var subscriptionRaftState = databaseRecord.Subscriptions[id.ToString()];
                criteria = subscriptionRaftState.Criteria;
                startChangeVector = subscriptionRaftState.ChangeVector;
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

        public IEnumerable<DynamicJsonValue> GetAllSubscriptions(TransactionOperationContext serverStoreContext, bool history, int start, int take)
        {
            var databaseRecord = _serverStore.Cluster.Read(serverStoreContext, _db.Name);

            databaseRecord.TryGet(nameof(DatabaseRecord.Subscriptions), out BlittableJsonReaderArray subscriptionsBlittable);

            foreach (BlittableJsonReaderObject curSubscriptionKeyValuePair in subscriptionsBlittable)
            {
                curSubscriptionKeyValuePair.TryGet("Key", out long subscriptionId);
                curSubscriptionKeyValuePair.TryGet("Value", out BlittableJsonReaderObject curSubscription);

                yield return GetSubscriptionInternal(subscriptionId, curSubscription, history);
            }
        }

        private DynamicJsonValue GetSubscriptionConnection(SubscriptionConnection connection)
        {
            return new DynamicJsonValue
            {
                [nameof(SubscriptionConnection.ClientUri)] = connection.ClientUri,
                [nameof(SubscriptionConnection.ConnectionException)] = connection.ConnectionException?.ToString(),
                [nameof(SubscriptionConnection.Stats)] = GetSubscriptionConnectionStats(connection.Stats),
                [nameof(SubscriptionConnection.Options)] = GetSubscriptionConnectionOptions(connection.Options)
            };
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

        public IEnumerable<DynamicJsonValue> GetAllRunningSubscriptions(TransactionOperationContext context, bool history, int start, int take)
        {            
            var databaseRecord = _serverStore.Cluster.Read(context, _db.Name);
                
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
                
                BlittableJsonReaderObject currentSubscriptionReader = 
                    GetSubscriptionFromDatabaseRecordBlittable(databaseRecord, subscriptionId);                

                Debug.Assert(currentSubscriptionReader != null);

                yield return GetRunningSubscriptionInternal(history, currentSubscriptionReader, subscriptionState);
            }

        }

        private static BlittableJsonReaderObject GetSubscriptionFromDatabaseRecordBlittable(BlittableJsonReaderObject databaseRecord, long subscriptionId)
        {
            BlittableJsonReaderObject foundSubscriptionReader = null;
            databaseRecord.TryGet(nameof(DatabaseRecord.Subscriptions), out BlittableJsonReaderArray subscriptionsReader);

            int low = 0, high = subscriptionsReader.Length - 1, midpoint = 0;

            while (low <= high)
            {
                midpoint = low + (high - low) / 2;

                var curSubscriptionKeyValuePair = subscriptionsReader[midpoint] as BlittableJsonReaderObject;
                curSubscriptionKeyValuePair.TryGet("Key", out long curSubscriptionId);

                if (subscriptionId == curSubscriptionId)
                {
                    curSubscriptionKeyValuePair.TryGet("Value", out foundSubscriptionReader);
                    low = high + 1;
                }
                else if (subscriptionId < curSubscriptionId)
                    high = midpoint - 1;
                else
                    low = midpoint + 1;
            }

            return foundSubscriptionReader;
        }

        public unsafe DynamicJsonValue GetSubscription(TransactionOperationContext context, long id, bool history)
        {
            var databaseRecord = _serverStore.Cluster.Read(context, _db.Name);

            var subscriptionBlittable = GetSubscriptionFromDatabaseRecordBlittable(databaseRecord, id);

            return GetSubscriptionInternal(id, subscriptionBlittable, history);
        }

        public DynamicJsonValue GetRunningSubscription(TransactionOperationContext context, long id, bool history)
        {
            SubscriptionState subscriptionState;
            if (_subscriptionStates.TryGetValue(id, out subscriptionState) == false)
                return null;

            if (subscriptionState.Connection == null)
                return null;


            var databaseRecord = _serverStore.Cluster.Read(context, _db.Name);

            var subscriptionBlittable = GetSubscriptionFromDatabaseRecordBlittable(databaseRecord, id);
            return GetRunningSubscriptionInternal(history, subscriptionBlittable, subscriptionState);            
        }

        public DynamicJsonValue GetRunningSubscriptionConnectionHistory(TransactionOperationContext context, long subscriptionId)
        {
            SubscriptionState subscriptionState;
            if (!_subscriptionStates.TryGetValue(subscriptionId, out subscriptionState))
                return null;

            var subscriptionConnection = subscriptionState.Connection;
            if (subscriptionConnection == null)
                return null;

            var databaseRecord = _serverStore.Cluster.Read(context, _db.Name);
            var subscriptionBlittable = GetSubscriptionFromDatabaseRecordBlittable(databaseRecord, subscriptionId);
            
            var subscriptionData = new DynamicJsonValue(subscriptionBlittable);
            SetSubscriptionStateData(subscriptionState, subscriptionData);
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

        private void SetSubscriptionStateData(SubscriptionState subscriptionState, DynamicJsonValue subscriptionData)
        {
            var subscriptionConnection = subscriptionState.Connection;
            subscriptionData[nameof(SubscriptionState.Connection)] = subscriptionConnection != null ? GetSubscriptionConnection(subscriptionConnection) : null;
        }

        private void SetSubscriptionHistory(SubscriptionState subscriptionState, DynamicJsonValue subscriptionData)
        {
            var recentConnections = new DynamicJsonArray();
            subscriptionData["RecentConnections"] = recentConnections;

            foreach (var connection in subscriptionState.RecentConnections)
            {
                recentConnections.Add(GetSubscriptionConnection(connection));
            }

            var rejectedConnections = new DynamicJsonArray();
            subscriptionData["RecentRejectedConnections"] = rejectedConnections;
            foreach (var connection in subscriptionState.RecentRejectedConnections)
            {
                rejectedConnections.Add(GetSubscriptionConnection(connection));
            }

        }

        private DynamicJsonValue GetRunningSubscriptionInternal(bool history, BlittableJsonReaderObject subscriptionReader, SubscriptionState subscriptionState)
        {            
            var subscriptionData = new DynamicJsonValue(subscriptionReader);          

            SetSubscriptionStateData(subscriptionState, subscriptionData);

            if (history)
                SetSubscriptionHistory(subscriptionState, subscriptionData);
            return subscriptionData;
        }

        private DynamicJsonValue GetSubscriptionInternal(long id , BlittableJsonReaderObject subscriptionRaftState, bool history)
        {
            var subscriptionData = new DynamicJsonValue(subscriptionRaftState);

            SubscriptionState subscriptionState;
            if (_subscriptionStates.TryGetValue(id, out subscriptionState))
            {
                SetSubscriptionStateData(subscriptionState, subscriptionData);

                if (history)
                    SetSubscriptionHistory(subscriptionState, subscriptionData);
            }
            else
            {
                // always include property in output json
                subscriptionData[nameof(SubscriptionState.Connection)] = null;
            }

            return subscriptionData;
        }       
    }

    // ReSharper disable once ClassNeverInstantiated.Local
    public static class SubscriptionSchema
    {
        public const string IdsTree = "SubscriptionsIDs";
        public const string SubsTree = "Subscriptions";
        public static readonly Slice Id;

        static SubscriptionSchema()
        {
            Slice.From(StorageEnvironment.LabelsContext, "Id", ByteStringType.Immutable, out Id);
        }

        public static class SubscriptionTable
        {
#pragma warning disable 169
            public const int IdIndex = 0;
            public const int CriteriaIndex = 1;
            public const int AckEtagIndex = 2;
            public const int TimeOfReceivingLastAck = 3;
#pragma warning restore 169
        }
    }
}
 