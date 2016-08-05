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

//using SubscriptionTable = Raven.Server.Documents.SubscriptionStorage.Schema.SubscriptionTable;

namespace Raven.Server.Documents
{

    // todo: implement functionality for limiting amount of opened subscriptions
    // todo: implement functionality for removing "old" subscriptions using dedicated index
    public class SubscriptionStorage : IDisposable
    {
        public static TimeSpan TwoMinutesTimespan = TimeSpan.FromMinutes(2);
        private readonly ConcurrentDictionary<long, SubscriptionState> _subscriptionStates = new ConcurrentDictionary<long, SubscriptionState>();
        private readonly TableSchema _subscriptionsSchema = new TableSchema();
        private readonly DocumentDatabase _db;
        private readonly MetricsScheduler _metricsScheduler;
        private readonly StorageEnvironment _environment;
        private Sparrow.Logging.Logger _logger; //todo: add logging

        private readonly UnmanagedBuffersPool _unmanagedBuffersPool;

        public SubscriptionStorage(DocumentDatabase db, MetricsScheduler metricsScheduler)
        {
            _db = db;
            _metricsScheduler = metricsScheduler;
            //TODO: You aren't copying all the other details from the configuration
            var options = _db.Configuration.Core.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly()
                : StorageEnvironmentOptions.ForPath(Path.Combine(_db.Configuration.Core.DataDirectory, "Subscriptions"));

            _environment = new StorageEnvironment(options);
            _unmanagedBuffersPool = new UnmanagedBuffersPool($"Subscriptions");

            var databaseName = db.Name;
            _logger = LoggerSetup.Instance.GetLogger<SubscriptionStorage>(databaseName);
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
                tx.CreateTree(Schema.IdsTree);
                _subscriptionsSchema.Create(tx, Schema.SubsTree);

                tx.Commit();
            }
        }

        public unsafe long CreateSubscription(BlittableJsonReaderObject criteria, long ackEtag=0)
        {

            // Validate that this can be properly parsed into a criteria object
            // and doing that without holding the tx lock
            JsonDeserialization.SubscriptionCriteria(criteria);

            using (var tx = _environment.WriteTransaction())
            {
                var table = new Table(_subscriptionsSchema, Schema.SubsTree, tx);
                var subscriptionsTree = tx.ReadTree(Schema.IdsTree);
                var id = subscriptionsTree.Increment(Schema.Id, 1);

                long timeOfSendingLastBatch = 0;
                long timeOfLastClientActivity = 0;

                var bigEndianId = Bits.SwapBytes((ulong)id);

                table.Insert(new TableValueBuilder
                {
                    &bigEndianId,
                    {criteria.BasePointer, criteria.Size},
                    &ackEtag,
                    &timeOfSendingLastBatch,
                    &timeOfLastClientActivity
                });
                tx.Commit();
                return id;
            }
        }

        public unsafe void UpdateSubscriptionTimes(long id, 
            bool updateLastBatch,
            bool updateClientActivity)
        {
            using (var innerTransaction = _environment.WriteTransaction())
            {
                var table = new Table(_subscriptionsSchema, Schema.SubsTree, innerTransaction);
                var subscriptionId = Bits.SwapBytes(id);
                var oldValue = table.ReadByKey(Slice.External(innerTransaction.Allocator, (byte*)&subscriptionId, sizeof (long)));

                if (oldValue == null)
                    throw new ArgumentException($"Cannot update subscription with id {id}, because it was not found");

                int size;

                var lastBatch = updateLastBatch
                    ? SystemTime.UtcNow.Ticks
                    : *(long*)oldValue.Read(Schema.SubscriptionTable.TimeOfSendingLastBatch, out size);

                var lastClient = updateClientActivity
                    ? SystemTime.UtcNow.Ticks
                    : *(long*)oldValue.Read(Schema.SubscriptionTable.TimeOfLastActivityIndex, out size);

                var tvb = new TableValueBuilder
                {
                    {&subscriptionId},
                    {oldValue.Read(Schema.SubscriptionTable.CriteriaIndex, out size), size},
                    {oldValue.Read(Schema.SubscriptionTable.AckEtagIndex, out size), sizeof (long)},
                    {&lastClient},
                    {&lastBatch}
                };
                table.Update(oldValue.Id, tvb);
                innerTransaction.Commit();
            }
        }

        public SubscriptionState OpenSubscription(SubscriptionConnection connection)
        {
            return _subscriptionStates.GetOrAdd(connection.SubscriptionId,
                _ => new SubscriptionState(connection));
        }


        public unsafe void AcknowledgeBatchProcessed(long id, long lastEtag)
        {
            using (var tx = _environment.WriteTransaction())
            {
                var config = GetSubscriptionConfig(id, tx);

                var subscriptionId = Bits.SwapBytes((ulong)id); ;

                int oldCriteriaSize;
                int longSizeForOutput;
                var now = SystemTime.UtcNow.Ticks;

                //todo: remove one of the time fields
                var tvb = new TableValueBuilder
                {
                    {(byte*)&subscriptionId, sizeof (long)},
                    {config.Read(Schema.SubscriptionTable.CriteriaIndex, out oldCriteriaSize), oldCriteriaSize},
                    {(byte*)&lastEtag, sizeof (long)},
                    {config.Read(Schema.SubscriptionTable.TimeOfSendingLastBatch, out longSizeForOutput), sizeof (long)},
                    {(byte*)&now, sizeof (long)}
                };
                var table = new Table(_subscriptionsSchema, Schema.SubsTree, tx);
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
                var criteriaPtr = config.Read(Schema.SubscriptionTable.CriteriaIndex, out criteriaSize);
                var criteriaBlittable = new BlittableJsonReaderObject(criteriaPtr, criteriaSize, context);
                criteria = JsonDeserialization.SubscriptionCriteria(criteriaBlittable);
                startEtag = *(long*)config.Read(Schema.SubscriptionTable.AckEtagIndex, out criteriaSize);
            }
        }

        private unsafe TableValueReader GetSubscriptionConfig(long id, Transaction tx)
        {
            var table = new Table(_subscriptionsSchema, Schema.SubsTree, tx);
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
                var table = new Table(_subscriptionsSchema, Schema.SubsTree, tx);
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
                var table = new Table(_subscriptionsSchema, Schema.SubsTree, tx);

                long subscriptionId = id;
                TableValueReader subscription = table.ReadByKey(Slice.External(tx.Allocator, (byte*)&subscriptionId, sizeof(ulong)));
                table.Delete(subscription.Id);

                tx.Commit();
            }
        }

        public void DropSubscriptionConnection(long subscriptionId)
        {
            SubscriptionState subscriptionState;
            if (_subscriptionStates.TryGetValue(subscriptionId, out subscriptionState))
            {
                subscriptionState.Connection.ConnectionException = new SubscriptionClosedException("Closed by request");
                subscriptionState.Connection.CancellationTokenSource.Cancel();
            }
        }

        private unsafe DynamicJsonValue ExtractSubscriptionConfigValue(TableValueReader tvr, DocumentsOperationContext context)
        {
            int size;
            var subscriptionId =
                Bits.SwapBytes(*(long*)tvr.Read(Schema.SubscriptionTable.IdIndex, out size));
            var ackEtag =
                *(long*)tvr.Read(Schema.SubscriptionTable.AckEtagIndex, out size);
            var timeOfSendingLastBatch =
                *(long*)tvr.Read(Schema.SubscriptionTable.TimeOfSendingLastBatch, out size);
            var timeOfLastClientActivity =
                *(long*)tvr.Read(Schema.SubscriptionTable.TimeOfLastActivityIndex, out size);
            var criteria = new BlittableJsonReaderObject(tvr.Read(Schema.SubscriptionTable.CriteriaIndex, out size), size, context);
            
            return new DynamicJsonValue
                {
                    ["SubscriptionId"] = subscriptionId,
                    ["Criteria"] = criteria,
                    ["AckEtag"] = ackEtag,
                    ["TimeOfSendingLastBatch"] = new DateTime(timeOfSendingLastBatch).ToString(CultureInfo.InvariantCulture),
                    ["TimeOfLastClientActivity"] = new DateTime(timeOfLastClientActivity).ToString(CultureInfo.InvariantCulture),
                };
        }

        private void WriteSubscriptionStateData(SubscriptionState subscriptionState, DynamicJsonValue subscriptionData, bool writeHistory = false)
        {
            var subscriptionConnection = subscriptionState.Connection;
            if (subscriptionConnection != null)
                SetSubscriptionConnectionStats(subscriptionConnection, subscriptionData);

            if (!writeHistory) return;

            subscriptionData["RecentConnections"] = new DynamicJsonArray(subscriptionState.RecentConnections.Select(
                connection =>
                {
                    var connectionStats = new DynamicJsonValue();
                    SetSubscriptionConnectionStats(connection, connectionStats);
                    return connectionStats;
                }));

            subscriptionData["RecentRejectedConnections"] =
                new DynamicJsonArray(subscriptionState.RecentRejectedConnections.Select(
                    connection =>
                    {
                        var connectionStats = new DynamicJsonValue();
                        SetSubscriptionConnectionStats(connection, connectionStats);
                        return connectionStats;
                    }));
        }

        public unsafe void GetAllSubscriptions(BlittableJsonTextWriter writer,
            DocumentsOperationContext context, int start, int take)
        {
            using (var tx = _environment.WriteTransaction())
            {
                var subscriptions = new List<DynamicJsonValue>();
                var table = new Table(_subscriptionsSchema, Schema.SubsTree, tx);
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
                        Bits.SwapBytes(*(long*)subscriptionTvr.Read(Schema.SubscriptionTable.IdIndex, out size));
                    SubscriptionState subscriptionState = null;

                    if (_subscriptionStates.TryGetValue(subscriptionId, out subscriptionState))
                    {
                        WriteSubscriptionStateData(subscriptionState, subscriptionData);
                    }

                    taken++;

                    if (taken > take)
                        break;
                }
                context.Write(writer, new DynamicJsonArray(subscriptions));
                writer.Flush();
            }
        }

        private void SetSubscriptionConnectionStats(SubscriptionConnection connection, DynamicJsonValue config)
        {
            config["ClientUri"] = connection.ClientEndpoint.ToString();
            config["TimeWaitedForConnectionToBeOvertaken"] = new TimeSpan(connection.Stats.WaitedForConnection).ToString();
            config["ConnectedAt"] = new DateTime(connection.Stats.ConnectedAt).ToString(CultureInfo.InvariantCulture);
            config["ConnectionException"] = connection.ConnectionException;

            config["LastMessageSentAt"] = new DateTime(connection.Stats.LastMessageSentAt).ToString(CultureInfo.InvariantCulture); 
            config["LastAckReceivedAt"] = new DateTime(connection.Stats.LastAckReceivedAt).ToString(CultureInfo.InvariantCulture); 

            config["DocsRate"] = connection.Stats.DocsRate.CreateMeterData();
            config["BytesRate"] = connection.Stats.BytesRate.CreateMeterData();
            config["AckRate"] = connection.Stats.AckRate.CreateMeterData();
        }

        public void GetRunningSusbscriptions(BlittableJsonTextWriter writer,
           DocumentsOperationContext context, int start, int take)
        {
            using (var tx = _environment.ReadTransaction())
            {
                var connections =
                    _subscriptionStates
                    .Where(x => x.Value.Connection != null)
                    .OrderBy(x=>x.Key)
                    .Skip(start)
                    .Take(take)
                    .Select(x =>
                        {
                            var subscriptionState = x.Value;
                            var subscriptionId = x.Key;
                            if (subscriptionState.Connection == null)
                                return null;

                            var subscriptionData = ExtractSubscriptionConfigValue(GetSubscriptionConfig(subscriptionId, tx), context);
                            WriteSubscriptionStateData(subscriptionState, subscriptionData);
                            return subscriptionData;
                    });


                context.Write(writer, new DynamicJsonArray(connections));
                writer.Flush();
            }
        }

        public void GetRunningSubscriptionConnectionHistory(BlittableJsonTextWriter writer, DocumentsOperationContext context, long subscriptionId)
        {
            SubscriptionState subscriptionState;
            if (!_subscriptionStates.TryGetValue(subscriptionId, out subscriptionState)) return;

            var subscriptionConnection = subscriptionState.Connection;
            if (subscriptionConnection == null) return;

            using (var tx = _environment.ReadTransaction())
            {
                var subscriptionData =
                    ExtractSubscriptionConfigValue(GetSubscriptionConfig(subscriptionId, tx), context);
                WriteSubscriptionStateData(subscriptionState, subscriptionData, true);

                context.Write(writer, subscriptionData);
                writer.Flush();
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
                var table = new Table(_subscriptionsSchema, Schema.SubsTree, tx);
                return table.NumberOfEntries;
            }
        }
    }

    // ReSharper disable once ClassNeverInstantiated.Local
    public class Schema
    {
        public static readonly string IdsTree = "SubscriptionsIDs";
        public static readonly string SubsTree = "Subscriptions";
        public static readonly Slice Id = Slice.From(StorageEnvironment.LabelsContext, "Id", ByteStringType.Immutable);

        public static class SubscriptionTable
        {
#pragma warning disable 169
            public static readonly int IdIndex = 0;
            public static readonly int CriteriaIndex = 1;
            public static readonly int AckEtagIndex = 2;
            public static readonly int TimeOfSendingLastBatch = 3;
            public static readonly int TimeOfLastActivityIndex = 4;
#pragma warning restore 169
        }
    }
}