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
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents.Subscriptions
{
    // todo: implement functionality for limiting amount of opened subscriptions
    public class SubscriptionStorage : IDisposable
    {
        private readonly DocumentDatabase _db;
        public static TimeSpan TwoMinutesTimespan = TimeSpan.FromMinutes(2);
        private readonly ConcurrentDictionary<long, SubscriptionState> _subscriptionStates = new ConcurrentDictionary<long, SubscriptionState>();
        private readonly TableSchema _subscriptionsSchema = new TableSchema();
        private readonly StorageEnvironment _environment;
        private readonly Logger _logger;


        public SubscriptionStorage(DocumentDatabase db)
        {
            _db = db;
            var path = db.Configuration.Core.DataDirectory.Combine("Subscriptions");

            var options = db.Configuration.Core.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly(path.FullPath, null, db.IoChanges, db.CatastrophicFailureNotification)
                : StorageEnvironmentOptions.ForPath(path.FullPath, null, null, db.IoChanges, db.CatastrophicFailureNotification);

            options.OnNonDurableFileSystemError += db.HandleNonDurableFileSystemError;
            options.OnRecoveryError += db.HandleOnRecoveryError;

            options.SchemaVersion = 1;
            options.TransactionsMode = TransactionsMode.Lazy;
            options.ForceUsing32BitsPager = db.Configuration.Storage.ForceUsing32BitsPager;
            options.TimeToSyncAfterFlashInSeconds = db.Configuration.Storage.TimeToSyncAfterFlashInSeconds;
            options.NumOfCocurrentSyncsPerPhysDrive = db.Configuration.Storage.NumOfCocurrentSyncsPerPhysDrive;
            options.MasterKey = db.MasterKey;


            _environment = new StorageEnvironment(options);
            var databaseName = db.Name;

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

                TableValueBuilder tableValueBuilder;
                using (table.Allocate(out tableValueBuilder))
                {
                    tableValueBuilder.Add(bigEndianId);
                    tableValueBuilder.Add(criteria.BasePointer, criteria.Size);
                    tableValueBuilder.Add(ackEtag);
                    tableValueBuilder.Add(timeOfSendingLastBatch);
                    tableValueBuilder.Add(timeOfLastClientActivity);
                    table.Insert(tableValueBuilder);
                }
                tx.Commit();
                if (_logger.IsInfoEnabled)
                    _logger.Info($"New Subscription With ID {id} was created");

                return id;
            }
        }

        public SubscriptionState OpenSubscription(SubscriptionConnection connection)
        {
            var subscriptionState = _subscriptionStates.GetOrAdd(connection.SubscriptionId,
                _ => new SubscriptionState(this));
            return subscriptionState;
        }

        public unsafe void AcknowledgeBatchProcessed(long id, long lastEtag)
        {
            var transactionPersistentContext = new TransactionPersistentContext();
            using (var tx = _environment.WriteTransaction(transactionPersistentContext))
            {
                TableValueReader config;
                GetSubscriptionConfig(id, tx, out config);

                var subscriptionId = Bits.SwapBytes((ulong)id);

                int oldCriteriaSize;
                var now = SystemTime.UtcNow.Ticks;
                var ptr = config.Read(SubscriptionSchema.SubscriptionTable.CriteriaIndex, out oldCriteriaSize);

                JsonOperationContext context;
                using (_db.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                {
                    var copy = context.GetMemory(oldCriteriaSize);
                    Memory.Copy(copy.Address, ptr, oldCriteriaSize);
                    var table = tx.OpenTable(_subscriptionsSchema, SubscriptionSchema.SubsTree);
                    TableValueBuilder tvb;
                    using (table.Allocate(out tvb))
                    {
                        tvb.Add((byte*)&subscriptionId, sizeof(long));
                        tvb.Add(copy.Address, oldCriteriaSize);
                        tvb.Add((byte*)&lastEtag, sizeof(long));
                        tvb.Add((byte*)&now, sizeof(long));
                        TableValueReader existingSubscription;
                        Slice subscriptionSlice;
                        using (Slice.External(tx.Allocator, (byte*)&subscriptionId, sizeof(long), out subscriptionSlice))
                        {
                            if (table.ReadByKey(subscriptionSlice, out existingSubscription) == false)
                                return;
                        }
                        table.Update(existingSubscription.Id, tvb);
                    }
                    tx.Commit();
                }
            }
        }

        public unsafe void GetCriteriaAndEtag(long id, DocumentsOperationContext context, out SubscriptionCriteria criteria, out long startEtag)
        {
            var transactionPersistentContext = new TransactionPersistentContext();
            using (var tx = _environment.ReadTransaction(transactionPersistentContext))
            {
                TableValueReader config;
                GetSubscriptionConfig(id, tx, out config);

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
            DropSubscriptionConnection(id, "Deleted");
            var transactionPersistentContext = new TransactionPersistentContext();
            using (var tx = _environment.WriteTransaction(transactionPersistentContext))
            {
                var table = tx.OpenTable(_subscriptionsSchema, SubscriptionSchema.SubsTree);

                var subscriptionId = Bits.SwapBytes(id);
                TableValueReader subscription;
                Slice subsriptionSlice;
                using (Slice.External(tx.Allocator, (byte*)&subscriptionId, sizeof(long), out subsriptionSlice))
                {
                    if (table.ReadByKey(subsriptionSlice, out subscription) == false)
                        return;

                    table.DeleteByKey(subsriptionSlice);
                }

                tx.Commit();

                if (_logger.IsInfoEnabled)
                {
                    _logger.Info($"Subscription with id {id} was deleted");
                }
            }
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

        public IEnumerable<DynamicJsonValue> GetAllSubscriptions(DocumentsOperationContext context, bool history, int start, int take)
        {
            var transactionPersistentContext = new TransactionPersistentContext();
            using (var tx = _environment.ReadTransaction(transactionPersistentContext))
            {
                var table = tx.OpenTable(_subscriptionsSchema, SubscriptionSchema.SubsTree);

                foreach (var subscriptionTvr in table.SeekByPrimaryKey(Slices.BeforeAllKeys, start))
                {
                    if (take-- <= 0)
                        yield break;

                    yield return GetSubscriptionInternal(context, ref subscriptionTvr.Reader, history);
                }
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
                TableValueReader reader;
                GetSubscriptionConfig(subscriptionId, tx, out reader);
                var subscriptionData = ExtractSubscriptionConfigValue(ref reader, context, out _);
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

        private static unsafe DynamicJsonValue ExtractSubscriptionConfigValue(ref TableValueReader tvr, JsonOperationContext context, out long id)
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

        private DynamicJsonValue GetRunningSubscriptionInternal(JsonOperationContext context, bool history, long id, Transaction tx, SubscriptionState subscriptionState)
        {
            long subscriptionId;
            TableValueReader result;
            GetSubscriptionConfig(id, tx, out result);
            var subscriptionData = ExtractSubscriptionConfigValue(ref result, context, out subscriptionId);
            Debug.Assert(id == subscriptionId);

            SetSubscriptionStateData(subscriptionState, subscriptionData);

            if (history)
                SetSubscriptionHistory(subscriptionState, subscriptionData);
            return subscriptionData;
        }

        private DynamicJsonValue GetSubscriptionInternal(JsonOperationContext context, ref TableValueReader tvr, bool history)
        {
            long id;
            var subscriptionData = ExtractSubscriptionConfigValue(ref tvr, context, out id);

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

        private unsafe void GetSubscriptionConfig(long id, Transaction tx, out TableValueReader result)
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
                result = config;
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
 