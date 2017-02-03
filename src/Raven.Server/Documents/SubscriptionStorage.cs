using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Json.Parsing;
using static Raven.Server.Utils.MetricsExtentions;

namespace Raven.Server.Documents
{
    // todo: implement functionality for limiting amount of opened subscriptions
    public class SubscriptionStorage : IDisposable
    {
        public static TimeSpan TwoMinutesTimespan = TimeSpan.FromMinutes(2);
        private readonly ConcurrentDictionary<long, SubscriptionState> _subscriptionStates = new ConcurrentDictionary<long, SubscriptionState>();
        private readonly TableSchema _subscriptionsSchema = new TableSchema();
        private readonly StorageEnvironment _environment;
        private readonly Logger _logger;

        private readonly UnmanagedBuffersPoolWithLowMemoryHandling _unmanagedBuffersPool;

        public SubscriptionStorage(DocumentDatabase db)
        {
            var path = db.Configuration.Core.DataDirectory.Combine("Subscriptions");

            var options = db.Configuration.Core.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly(path.FullPath)
                : StorageEnvironmentOptions.ForPath(path.FullPath);


            options.SchemaVersion = 1;
            options.TransactionsMode = TransactionsMode.Lazy;
            _environment = new StorageEnvironment(options);
            var databaseName = db.Name;
            _unmanagedBuffersPool = new UnmanagedBuffersPoolWithLowMemoryHandling("Subscriptions", databaseName);

            _logger = LoggingSource.Instance.GetLogger<SubscriptionStorage>(databaseName);
            _subscriptionsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1
            });
        }

        public void Dispose()
        {
            foreach (var state in _subscriptionStates.Values)
            {
                state.Dispose();
            }
            _unmanagedBuffersPool.Dispose();
            _environment.Dispose();
        }

        public void Initialize()
        {
            var transactionPersistentContext = new TransactionPersistentContext();
            using (var tx = _environment.WriteTransaction(transactionPersistentContext))
            {
                tx.CreateTree(SubscriptionSchema.IdsTree);
                _subscriptionsSchema.Create(tx, SubscriptionSchema.SubsTree, 16);

                tx.Commit();
            }
        }

        public unsafe long CreateSubscription(BlittableJsonReaderObject criteria, long ackEtag = 0)
        {
            // Validate that this can be properly parsed into a criteria object
            // and doing that without holding the tx lock
            JsonDeserializationServer.SubscriptionCriteria(criteria);

            var transactionPersistentContext = new TransactionPersistentContext();
            using (var tx = _environment.WriteTransaction(transactionPersistentContext))
            {
                var table = tx.OpenTable(_subscriptionsSchema, SubscriptionSchema.SubsTree);
                var subscriptionsTree = tx.ReadTree(SubscriptionSchema.IdsTree);
                var id = subscriptionsTree.Increment(SubscriptionSchema.Id, 1);

                const long timeOfSendingLastBatch = 0;
                const long timeOfLastClientActivity = 0;

                var bigEndianId = Bits.SwapBytes((ulong)id);

                table.Insert(new TableValueBuilder
                {
                    bigEndianId,
                    {criteria.BasePointer, criteria.Size},
                    ackEtag,
                    timeOfSendingLastBatch,
                    timeOfLastClientActivity
                });
                tx.Commit();
                if (_logger.IsInfoEnabled)
                    _logger.Info($"New Subscription With ID {id} was created");

                return id;
            }
        }

        public SubscriptionState OpenSubscription(SubscriptionConnection connection)
        {
            var subscriptionState = _subscriptionStates.GetOrAdd(connection.SubscriptionId,
                _ => new SubscriptionState());
            return subscriptionState;
        }

        public unsafe void AcknowledgeBatchProcessed(long id, long lastEtag)
        {
            var transactionPersistentContext = new TransactionPersistentContext();
            using (var tx = _environment.WriteTransaction(transactionPersistentContext))
            {
                var config = GetSubscriptionConfig(id, tx);

                var subscriptionId = Bits.SwapBytes((ulong)id);

                int oldCriteriaSize;
                var now = SystemTime.UtcNow.Ticks;

                var tvb = new TableValueBuilder
                {
                    {(byte*)&subscriptionId, sizeof (long)},
                    {config.Read(SubscriptionSchema.SubscriptionTable.CriteriaIndex, out oldCriteriaSize), oldCriteriaSize},
                    {(byte*)&lastEtag, sizeof (long)},
                    {(byte*)&now, sizeof (long)}
                };
                var table = tx.OpenTable(_subscriptionsSchema, SubscriptionSchema.SubsTree);
                TableValueReader existingSubscription;
                Slice subscriptionSlice;
                using (Slice.External(tx.Allocator, (byte*)&subscriptionId, sizeof(long), out subscriptionSlice))
                {
                    if (table.ReadByKey(subscriptionSlice, out existingSubscription) == false)
                        return;
                }
                table.Update(existingSubscription.Id, tvb);
                tx.Commit();
            }
        }

        public unsafe void GetCriteriaAndEtag(long id, DocumentsOperationContext context, out SubscriptionCriteria criteria, out long startEtag)
        {
            var transactionPersistentContext = new TransactionPersistentContext();
            using (var tx = _environment.ReadTransaction(transactionPersistentContext))
            {
                var config = GetSubscriptionConfig(id, tx);

                int criteriaSize;
                var criteriaPtr = config.Read(SubscriptionSchema.SubscriptionTable.CriteriaIndex, out criteriaSize);
                using (var criteriaBlittable = new BlittableJsonReaderObject(criteriaPtr, criteriaSize, context))
                {
                    criteria = JsonDeserializationServer.SubscriptionCriteria(criteriaBlittable);
                    startEtag =
                        *(long*) config.Read(SubscriptionSchema.SubscriptionTable.AckEtagIndex, out criteriaSize);
                }
            }
        }

        public unsafe void AssertSubscriptionIdExists(long id)
        {
            var transactionPersistentContext = new TransactionPersistentContext();
            using (var tx = _environment.ReadTransaction(transactionPersistentContext))
            {
                var table = tx.OpenTable(_subscriptionsSchema, SubscriptionSchema.SubsTree);
                var subscriptionId = Bits.SwapBytes((ulong)id);

                Slice subsriptionSlice;
                using (Slice.External(tx.Allocator, (byte*)&subscriptionId, sizeof(long), out subsriptionSlice))
                {
                    if (table.VerifyKeyExists(subsriptionSlice) == false)
                        throw new SubscriptionDoesNotExistException(
                            "There is no subscription configuration for specified identifier (id: " + id + ")");
                }
            }

        }


        public unsafe void DeleteSubscription(long id)
        {
            SubscriptionState subscriptionState;
            if (_subscriptionStates.TryRemove(id, out subscriptionState))
            {
                subscriptionState.EndConnection();
                subscriptionState.Dispose();
            }

            var transactionPersistentContext = new TransactionPersistentContext();
            using (var tx = _environment.WriteTransaction(transactionPersistentContext))
            {
                var table = tx.OpenTable(_subscriptionsSchema, SubscriptionSchema.SubsTree);

                long subscriptionId = id;
                TableValueReader subscription;
                Slice subsriptionSlice;
                using (Slice.External(tx.Allocator, (byte*)&subscriptionId, sizeof(long), out subsriptionSlice))
                {
                    if (table.ReadByKey(subsriptionSlice, out subscription) == false)
                        return;
                }
                table.Delete(subscription.Id);

                tx.Commit();

                if (_logger.IsInfoEnabled)
                {
                    _logger.Info($"Subscription with id {id} was deleted");
                }
            }
        }

        public bool DropSubscriptionConnection(long subscriptionId)
        {
            SubscriptionState subscriptionState;
            if (_subscriptionStates.TryGetValue(subscriptionId, out subscriptionState) == false)
                return false;

            subscriptionState.Connection.ConnectionException = new SubscriptionClosedException("Closed by request");
            subscriptionState.RegisterRejectedConnection(subscriptionState.Connection, new SubscriptionClosedException("Closed by request"));
            subscriptionState.Connection.CancellationTokenSource.Cancel();

            if (_logger.IsInfoEnabled)
                _logger.Info($"Subscription with id {subscriptionId} connection was dropped");

            return true;
        }

        public IEnumerable<DynamicJsonValue> GetAllSubscriptions(DocumentsOperationContext context, bool history, int start, int take)
        {
            var transactionPersistentContext = new TransactionPersistentContext();
            using (var tx = _environment.ReadTransaction(transactionPersistentContext))
            {
                var table = tx.OpenTable(_subscriptionsSchema, SubscriptionSchema.SubsTree);

                foreach (var subscriptionTvr in table.SeekByPrimaryKey(Slices.BeforeAllKeys))
                {
                    if (start > 0)
                    {
                        start--;
                        continue;
                    }

                    if (take-- <= 0)
                        yield break;

                    yield return GetSubscriptionInternal(context, ref subscriptionTvr.Reader, history);
                }
            }
        }

        private void SetSubscriptionConnectionStats(SubscriptionConnection connection, DynamicJsonValue config)
        {
            config["ClientUri"] = connection.TcpConnection.TcpClient.Client.RemoteEndPoint.ToString();
            config["ConnectedAt"] = connection.Stats.ConnectedAt;
            config["ConnectionException"] = connection.ConnectionException;

            config["LastMessageSentAt"] = connection.Stats.LastMessageSentAt;
            config["LastAckReceivedAt"] = connection.Stats.LastAckReceivedAt;

            config["DocsRate"] = connection.Stats.DocsRate.CreateMeterData();
            config["BytesRate"] = connection.Stats.BytesRate.CreateMeterData();
            config["AckRate"] = connection.Stats.AckRate.CreateMeterData();
        }

        public IEnumerable<DynamicJsonValue> GetAllRunningSubscriptions(DocumentsOperationContext context, bool history, int start, int take)
        {
            var transactionPersistentContext = new TransactionPersistentContext();
            using (var tx = _environment.ReadTransaction(transactionPersistentContext))
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

                    yield return GetRunningSubscriptionInternal(context, history, subscriptionId, tx, subscriptionState);
                }
            }
        }

        public unsafe DynamicJsonValue GetSubscription(DocumentsOperationContext context, long id, bool history)
        {
            var transactionPersistentContext = new TransactionPersistentContext();
            using (var tx = _environment.ReadTransaction(transactionPersistentContext))
            {
                var table = tx.OpenTable(_subscriptionsSchema, SubscriptionSchema.SubsTree);

                Slice subscriptionSlice;
                using (Slice.External(tx.Allocator, (byte*)&id, sizeof(long), out subscriptionSlice))
                {
                    TableValueReader reader;
                    if (table.ReadByKey(subscriptionSlice, out reader) == false)
                        return null;

                    return GetSubscriptionInternal(context, ref reader, history);
                }
            }
        }

        public DynamicJsonValue GetRunningSubscription(DocumentsOperationContext context, long id, bool history)
        {
            SubscriptionState subscriptionState;
            if (_subscriptionStates.TryGetValue(id, out subscriptionState) == false)
                return null;

            if (subscriptionState.Connection == null)
                return null;

            var transactionPersistentContext = new TransactionPersistentContext();
            using (var tx = _environment.ReadTransaction(transactionPersistentContext))
            {
                return GetRunningSubscriptionInternal(context, history, id, tx, subscriptionState);
            }
        }

        public DynamicJsonValue GetRunningSubscriptionConnectionHistory(JsonOperationContext context, long subscriptionId)
        {
            SubscriptionState subscriptionState;
            if (!_subscriptionStates.TryGetValue(subscriptionId, out subscriptionState))
                return null;

            var subscriptionConnection = subscriptionState.Connection;
            if (subscriptionConnection == null)
                return null;

            var transactionPersistentContext = new TransactionPersistentContext();
            using (var tx = _environment.ReadTransaction(transactionPersistentContext))
            {
                long _;
                var subscriptionData = ExtractSubscriptionConfigValue(GetSubscriptionConfig(subscriptionId, tx), context, out _);
                SetSubscriptionStateData(subscriptionState, subscriptionData);
                SetSubscriptionHistory(subscriptionState, subscriptionData);

                return subscriptionData;
            }
        }

        public long GetRunningCount()
        {
            return _subscriptionStates.Count(x => x.Value.Connection != null);
        }


        public StorageEnvironment Environment()
        {
            return _environment;
        }

        public long GetAllSubscriptionsCount()
        {
            var transactionPersistentContext = new TransactionPersistentContext();
            using (var tx = _environment.ReadTransaction(transactionPersistentContext))
            {
                var table = tx.OpenTable(_subscriptionsSchema, SubscriptionSchema.SubsTree);
                return table.NumberOfEntries;
            }
        }

        private static unsafe DynamicJsonValue ExtractSubscriptionConfigValue(TableValueReader tvr, JsonOperationContext context, out long id)
        {
            int size;
            id = Bits.SwapBytes(*(long*)tvr.Read(SubscriptionSchema.SubscriptionTable.IdIndex, out size));
            var ackEtag = *(long*)tvr.Read(SubscriptionSchema.SubscriptionTable.AckEtagIndex, out size);
            var timeOfReceivingLastAck = *(long*)tvr.Read(SubscriptionSchema.SubscriptionTable.TimeOfReceivingLastAck, out size);
            var ptr = tvr.Read(SubscriptionSchema.SubscriptionTable.CriteriaIndex, out size);
            var data = context.GetMemory(size);
            Memory.Copy(data.Address, ptr, size);
            var criteria = new BlittableJsonReaderObject(data.Address, size, context);

            return new DynamicJsonValue
            {
                ["SubscriptionId"] = id,
                ["Criteria"] = criteria,
                ["AckEtag"] = ackEtag,
                ["TimeOfReceivingLastAck"] = new DateTime(timeOfReceivingLastAck).ToString(CultureInfo.InvariantCulture),
            };
        }

        private void SetSubscriptionStateData(SubscriptionState subscriptionState, DynamicJsonValue subscriptionData)
        {
            var subscriptionConnection = subscriptionState.Connection;
            if (subscriptionConnection != null)
                SetSubscriptionConnectionStats(subscriptionConnection, subscriptionData);

            SetSubscriptionHistory(subscriptionState, subscriptionData);
        }

        private void SetSubscriptionHistory(SubscriptionState subscriptionState, DynamicJsonValue subscriptionData)
        {
            var recentConnections = new DynamicJsonArray();
            subscriptionData["RecentConnections"] = recentConnections;

            foreach (var connection in subscriptionState.RecentConnections)
            {
                var connectionStats = new DynamicJsonValue();
                SetSubscriptionConnectionStats(connection, connectionStats);
                recentConnections.Add(connectionStats);
            }


            var rejectedConnections = new DynamicJsonArray();
            subscriptionData["RecentRejectedConnections"] = rejectedConnections;
            foreach (var connection in subscriptionState.RecentRejectedConnections)
            {
                var connectionStats = new DynamicJsonValue();
                SetSubscriptionConnectionStats(connection, connectionStats);
                rejectedConnections.Add(connectionStats);
            }

        }

        private DynamicJsonValue GetRunningSubscriptionInternal(JsonOperationContext context, bool history, long id, Transaction tx, SubscriptionState subscriptionState)
        {
            long subscriptionId;
            var subscriptionData = ExtractSubscriptionConfigValue(GetSubscriptionConfig(id, tx), context, out subscriptionId);
            Debug.Assert(id == subscriptionId);

            SetSubscriptionStateData(subscriptionState, subscriptionData);

            if (history)
                SetSubscriptionHistory(subscriptionState, subscriptionData);
            return subscriptionData;
        }

        private DynamicJsonValue GetSubscriptionInternal(JsonOperationContext context, ref TableValueReader tvr, bool history)
        {
            long id;
            var subscriptionData = ExtractSubscriptionConfigValue(tvr, context, out id);

            SubscriptionState subscriptionState;
            if (_subscriptionStates.TryGetValue(id, out subscriptionState))
            {
                SetSubscriptionStateData(subscriptionState, subscriptionData);

                if (history)
                    SetSubscriptionHistory(subscriptionState, subscriptionData);
            }

            return subscriptionData;
        }

        private unsafe TableValueReader GetSubscriptionConfig(long id, Transaction tx)
        {
            var table = tx.OpenTable(_subscriptionsSchema, SubscriptionSchema.SubsTree);
            var subscriptionId = Bits.SwapBytes((ulong)id);

            Slice subscriptionSlice;
            using (Slice.External(tx.Allocator, (byte*)&subscriptionId, sizeof(long), out subscriptionSlice))
            {
                TableValueReader config;
                if (table.ReadByKey(subscriptionSlice, out config) == false)
                    throw new SubscriptionDoesNotExistException(
                    "There is no subscription configuration for specified identifier (id: " + id + ")");
                return config;
            }

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