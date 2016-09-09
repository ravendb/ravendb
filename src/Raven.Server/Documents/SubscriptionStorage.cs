using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;

using NLog;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Database.Util;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Metrics;
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
        private readonly DocumentDatabase _db;
        private readonly StorageEnvironment _environment;
        private Sparrow.Logging.Logger _logger; 

        private readonly UnmanagedBuffersPoolWithLowMemoryHandling _unmanagedBuffersPool;

        public SubscriptionStorage(DocumentDatabase db)
        {
            _db = db;

            var options = _db.Configuration.Core.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly()
                : StorageEnvironmentOptions.ForPath(Path.Combine(_db.Configuration.Core.DataDirectory, "Subscriptions"));
            

            options.SchemaVersion = 1;
            options.TransactionsMode=TransactionsMode.Lazy;
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
            using (var tx = _environment.WriteTransaction())
            {
                tx.CreateTree(SubscriptionSchema.IdsTree);
                _subscriptionsSchema.Create(tx, SubscriptionSchema.SubsTree);

                tx.Commit();
            }
        }

        public unsafe long CreateSubscription(BlittableJsonReaderObject criteria, long ackEtag=0)
        {

            // Validate that this can be properly parsed into a criteria object
            // and doing that without holding the tx lock
            JsonDeserializationServer.SubscriptionCriteria(criteria);

            using (var tx = _environment.WriteTransaction())
            {
                var table = tx.OpenTable(_subscriptionsSchema, SubscriptionSchema.SubsTree);
                var subscriptionsTree = tx.ReadTree(SubscriptionSchema.IdsTree);
                var id = subscriptionsTree.Increment(SubscriptionSchema.Id, 1);

                long timeOfSendingLastBatch = 0;
                long timeOfLastClientActivity = 0;

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
                _ => new SubscriptionState(connection));
            return subscriptionState;
        }


        public unsafe void AcknowledgeBatchProcessed(long id, long lastEtag)
        {
            using (var tx = _environment.WriteTransaction())
            {
                var config = GetSubscriptionConfig(id, tx);

                var subscriptionId = Bits.SwapBytes((ulong)id); ;

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
                var existingSubscription = table.ReadByKey(Slice.External(tx.Allocator, (byte*)&subscriptionId, sizeof(long)));
                table.Update(existingSubscription.Id, tvb);
                tx.Commit();
            }
        }

        public void AssertSubscriptionExists(long id)
        {
            if (_subscriptionStates.ContainsKey(id) == false)
                throw new SubscriptionClosedException("There is no open subscription with id: " + id);
        }

        public unsafe void GetCriteriaAndEtag(long id, DocumentsOperationContext context, out SubscriptionCriteria criteria, out long startEtag)
        {
            using (var tx = _environment.ReadTransaction())
            {
                var config = GetSubscriptionConfig(id, tx);

                int criteriaSize;
                var criteriaPtr = config.Read(SubscriptionSchema.SubscriptionTable.CriteriaIndex, out criteriaSize);
                var criteriaBlittable = new BlittableJsonReaderObject(criteriaPtr, criteriaSize, context);
                criteria = JsonDeserializationServer.SubscriptionCriteria(criteriaBlittable);
                startEtag = *(long*)config.Read(SubscriptionSchema.SubscriptionTable.AckEtagIndex, out criteriaSize);
            }
        }

        private unsafe TableValueReader GetSubscriptionConfig(long id, Transaction tx)
        {
            var table = tx.OpenTable(_subscriptionsSchema, SubscriptionSchema.SubsTree);
            var subscriptionId = Bits.SwapBytes((ulong)id);

            var config = table.ReadByKey(Slice.External(tx.Allocator, (byte*)&subscriptionId, sizeof(long)));

            if (config == null)
                throw new SubscriptionDoesNotExistException(
                    "There is no subscription configuration for specified identifier (id: " + id + ")");
            return config;
        }

        public unsafe void AssertSubscriptionIdExists(long id)
        {
            using (var tx = _environment.ReadTransaction())
            {
                var table = tx.OpenTable(_subscriptionsSchema, SubscriptionSchema.SubsTree);
                var subscriptionId = Bits.SwapBytes((ulong)id);

                if (table.VerifyKeyExists(Slice.External(tx.Allocator, (byte*)&subscriptionId, sizeof(ulong))) == false)
                    throw new SubscriptionDoesNotExistException(
                        "There is no subscription configuration for specified identifier (id: " + id + ")");
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

            using (var tx = _environment.WriteTransaction())
            {
                var table = tx.OpenTable(_subscriptionsSchema, SubscriptionSchema.SubsTree);

                long subscriptionId = id;
                TableValueReader subscription = table.ReadByKey(Slice.External(tx.Allocator, (byte*)&subscriptionId, sizeof(ulong)));
                table.Delete(subscription.Id);

                tx.Commit();

                if (_logger.IsInfoEnabled)
                {
                    _logger.Info($"Subscription with id {id} was deleted");
                }
            }
        }

        public void DropSubscriptionConnection(long subscriptionId)
        {
            SubscriptionState subscriptionState;
            if (_subscriptionStates.TryGetValue(subscriptionId, out subscriptionState))
            {
                subscriptionState.Connection.ConnectionException = new SubscriptionClosedException("Closed by request");
                subscriptionState.RegisterRejectedConnection(subscriptionState.Connection,
                    new SubscriptionClosedException("Closed by request"));
                subscriptionState.Connection.CancellationTokenSource.Cancel();
            }
            if (_logger.IsInfoEnabled)
                _logger.Info($"Subscription with id {subscriptionId} connection was dropped");
        }

        private unsafe DynamicJsonValue ExtractSubscriptionConfigValue(TableValueReader tvr, JsonOperationContext context)
        {
            int size;
            var subscriptionId =
                Bits.SwapBytes(*(long*)tvr.Read(SubscriptionSchema.SubscriptionTable.IdIndex, out size));
            var ackEtag =
                *(long*)tvr.Read(SubscriptionSchema.SubscriptionTable.AckEtagIndex, out size);
            var timeOfReceivingLastAck =
                *(long*)tvr.Read(SubscriptionSchema.SubscriptionTable.TimeOfReceivingLastAck, out size);
            var ptr = tvr.Read(SubscriptionSchema.SubscriptionTable.CriteriaIndex, out size);
            var data = context.GetMemory(size);
            Memory.Copy((byte*) data.Address, ptr, size);
            var criteria = new BlittableJsonReaderObject((byte*)data.Address, size, context);
            
            return new DynamicJsonValue
                {
                    ["SubscriptionId"] = subscriptionId,
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

        public unsafe void GetAllSubscriptions(BlittableJsonTextWriter writer,
            DocumentsOperationContext context, int start, int take)
        {
            using (var tx = _environment.WriteTransaction())
            {
                var subscriptions = new List<DynamicJsonValue>();
                var table = tx.OpenTable(_subscriptionsSchema, SubscriptionSchema.SubsTree);
                var seen = 0;
                var taken = 0;
                foreach (var subscriptionTvr in table.SeekByPrimaryKey(Slices.BeforeAllKeys))
                {
                    if (seen < start)
                    {
                        seen++;
                        continue;
                    }

                    var subscriptionData = ExtractSubscriptionConfigValue(subscriptionTvr,context);
                    subscriptions.Add(subscriptionData);
                    int size;
                    var subscriptionId = 
                        Bits.SwapBytes(*(long*)subscriptionTvr.Read(SubscriptionSchema.SubscriptionTable.IdIndex, out size));
                    SubscriptionState subscriptionState = null;

                    if (_subscriptionStates.TryGetValue(subscriptionId, out subscriptionState))
                    {
                        SetSubscriptionStateData(subscriptionState, subscriptionData);
                    }

                    taken++;

                    if (taken > take)
                        break;
                }
                context.Write(writer, new DynamicJsonArray(subscriptions));
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

        public void GetRunningSusbscriptions(BlittableJsonTextWriter writer,
           DocumentsOperationContext context, int start, int take)
        {
            using (var tx = _environment.ReadTransaction())
            {
                var connections = new List<DynamicJsonValue>(take);
                var skipped = 0;
                var taken = 0;
                foreach (var kvp in _subscriptionStates)
                {
                    var subscriptionState = kvp.Value;
                    var subscriptionId = kvp.Key;

                    if (taken > take)
                    {
                        break;
                    }

                    if (subscriptionState?.Connection == null)
                        continue;

                    if (skipped < start)
                    {
                        skipped++;
                        continue;
                    }

                    var subscriptionData = ExtractSubscriptionConfigValue(GetSubscriptionConfig(subscriptionId, tx), context);
                    SetSubscriptionStateData(subscriptionState, subscriptionData);
                    connections.Add(subscriptionData);
                    taken++;
                }

                context.Write(writer, new DynamicJsonArray(connections));
                writer.Flush();
            }
        }

        public DynamicJsonValue GetRunningSubscriptionConnectionHistory(JsonOperationContext context, long subscriptionId)
        {
            SubscriptionState subscriptionState;
            if (!_subscriptionStates.TryGetValue(subscriptionId, out subscriptionState)) return null;

            var subscriptionConnection = subscriptionState.Connection;
            if (subscriptionConnection == null) return null;

            using (var tx = _environment.ReadTransaction())
            {
                var subscriptionData =
                    ExtractSubscriptionConfigValue(GetSubscriptionConfig(subscriptionId, tx), context);
                SetSubscriptionStateData(subscriptionState, subscriptionData);
                SetSubscriptionHistory(subscriptionState, subscriptionData);

                return subscriptionData;
            }
        }
        
        public long GetRunningCount()
        {
            return _subscriptionStates.Count(x=>x.Value.Connection!=null);
        }


        public StorageEnvironment Environment()
        {
            return _environment;
        }
        public long GetAllSubscriptionsCount()
        {
            using (var tx = _environment.WriteTransaction())
            {
                var table = tx.OpenTable(_subscriptionsSchema, SubscriptionSchema.SubsTree);
                return table.NumberOfEntries;
            }
        }
    }

    // ReSharper disable once ClassNeverInstantiated.Local
    public static class SubscriptionSchema
    {
        public const string IdsTree = "SubscriptionsIDs";
        public const string SubsTree = "Subscriptions";
        public static readonly Slice Id = Slice.From(StorageEnvironment.LabelsContext, "Id", ByteStringType.Immutable);

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