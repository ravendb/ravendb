using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using NLog;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Database.Util;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
//using SubscriptionTable = Raven.Server.Documents.SubscriptionStorage.Schema.SubscriptionTable;

namespace Raven.Server.Documents
{
    public class SubscriptionStorage : IDisposable
    {
        public static TimeSpan TwoMinutesTimespan = TimeSpan.FromMinutes(2);

        private readonly TransactionContextPool _contextPool;

        private readonly ConcurrentDictionary<long, SizeLimitedConcurrentSet<string>> _forciblyReleasedSubscriptions =
            new ConcurrentDictionary<long, SizeLimitedConcurrentSet<string>>();

        private readonly ConcurrentDictionary<long, SubscriptionConnectionOptions> _openSubscriptions = new ConcurrentDictionary<long, SubscriptionConnectionOptions>();


        private readonly JsonOperationContext _subscriptionsContext;
        private readonly TableSchema _subscriptionsSchema = new TableSchema();
        private readonly DocumentDatabase _db;
        private readonly StorageEnvironment _environment;
        private Logger _log; //todo: add logging

        private readonly UnmanagedBuffersPool _unmanagedBuffersPool;

        private static readonly Func<BlittableJsonReaderObject, SubscriptionCriteria> _subscriptionCriteriaDeserializer =
            JsonDeserialization.SubscriptionCriteria;

        private static readonly Func<BlittableJsonReaderObject, SubscriptionConnectionOptions>
            _subscriptionConnectionOptionsDeserializer = JsonDeserialization.SubscriptionCriteriaOptions;



        public SubscriptionStorage(DocumentDatabase db)
        {
            _db = db;
            var options = _db.Configuration.Core.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly()
                : StorageEnvironmentOptions.ForPath(Path.Combine(_db.Configuration.Core.DataDirectory, "Subscriptions"));

            _environment = new StorageEnvironment(options);
            _unmanagedBuffersPool = new UnmanagedBuffersPool($"Subscriptions");
            _contextPool = new TransactionContextPool(_unmanagedBuffersPool, _environment);

            _contextPool.AllocateOperationContext(out _subscriptionsContext);
            var databaseName = db.Name;
            _log = LogManager.GetLogger($"{typeof(SubscriptionStorage).FullName}.{databaseName}");
            _subscriptionsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1
            });
        }

        public void Dispose()
        {
            _subscriptionsContext.Dispose();
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
            using (var tx = _environment.WriteTransaction())
            {
                var table = new Table(_subscriptionsSchema, Schema.SubsTree, tx);
                var subscriptionsTree = tx.ReadTree(Schema.IdsTree);
                var id = subscriptionsTree.Increment(Schema.Id, 1);

                long timeOfSendingLastBatch = 0;
                long timeOfLastClientActivity = 0;
                

                _subscriptionCriteriaDeserializer(criteria);

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

        public unsafe void UpdateSubscriptionTimes(long id, bool updateLastBatch, bool updateClientActivity, Transaction outerTransaction = null)
        {
            Transaction innerTransaction = outerTransaction;
            if (outerTransaction == null)
                innerTransaction = _environment.WriteTransaction();

            try
            {
                var table = new Table(_subscriptionsSchema, Schema.SubsTree, innerTransaction);
                var subscriptionId = Bits.SwapBytes(id);
                var oldValue = table.ReadByKey(new Slice((byte*)&subscriptionId, sizeof(long)));

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

            }
            finally
            {
                if (outerTransaction == null)
                {
                    innerTransaction.Commit();
                    innerTransaction.Dispose();
                }
            }


        }

        private void ForceReleaseAndOpenForNewClient(long id, SubscriptionConnectionOptions options, Transaction tx)
        {
            ReleaseSubscription(id);
            _openSubscriptions.TryAdd(id, options);
            UpdateSubscriptionTimes(id, updateClientActivity: true, updateLastBatch: false, outerTransaction: tx);
        }

        private unsafe Tuple<BlittableJsonReaderObject, UnmanagedBuffersPool.AllocatedMemoryData>
            AllocateReaderObjectCopy(BlittableJsonReaderObject subscriptionConnectionOptions)
        {
            var allocatedMemory = _subscriptionsContext.GetMemory(subscriptionConnectionOptions.Size);
            subscriptionConnectionOptions.CopyTo((byte*)allocatedMemory.Address);
            var readerObjectCopy = new BlittableJsonReaderObject((byte*)allocatedMemory.Address,
                subscriptionConnectionOptions.Size, _subscriptionsContext);
            return Tuple.Create(readerObjectCopy, allocatedMemory);
        }

        public unsafe void OpenSubscription(long id, BlittableJsonReaderObject connectionOptionsBlittable)
        {
            SizeLimitedConcurrentSet<string> releasedConnections;

            SubscriptionOpeningStrategy subscriptionConnectionStrategy;

            connectionOptionsBlittable.TryGet("Strategy", out subscriptionConnectionStrategy);

            var connectionOptions = _subscriptionConnectionOptionsDeserializer(connectionOptionsBlittable);

            var connectionId = connectionOptions.ConnectionId;

            if (_forciblyReleasedSubscriptions.TryGetValue(id, out releasedConnections) &&
                releasedConnections.Contains(connectionId))
                throw new SubscriptionClosedException("Subscription " + id + " was forcibly released. Cannot reopen it.");


            // if subscription is not opened, store subscription connection options and update subscription activity
            if (_openSubscriptions.TryAdd(id, connectionOptions))
            {
                UpdateSubscriptionTimes(id, updateLastBatch: false, updateClientActivity: true);
                return;
            }

            // check if there is already opened subscription connection with the same id
            SubscriptionConnectionOptions existingOptions;

            if (_openSubscriptions.TryGetValue(id, out existingOptions) == false)
                throw new SubscriptionDoesNotExistException(
                    "Didn't get existing open subscription while it's expected. Subscription id: " + id);

            var existingOptionsConnectionId = existingOptions.ConnectionId;

            if (existingOptionsConnectionId.Equals(connectionId, StringComparison.OrdinalIgnoreCase))
            {
                // reopen subscription on already existing connection - might happen after network connection problems the client tries to reopen
                UpdateSubscriptionTimes(id, updateLastBatch: false, updateClientActivity: true);
                return;
            }

            using (var tx = _environment.WriteTransaction())
            {
                var table = new Table(_subscriptionsSchema, Schema.SubsTree, tx);
                var subscriptionId = id;
                var config = table.ReadByKey(new Slice((byte*)&subscriptionId, sizeof(long)));
                var now = SystemTime.UtcNow.Ticks;
                int readSize;
                var timeSinceBatchSentTicks = now -
                                              *(long*)
                                                      config.Read(Schema.SubscriptionTable.TimeOfSendingLastBatch,
                                                          out readSize);

                var acknowledgementTimeoutTicks = existingOptions.AcknowledgmentTimeout;
                int tempSize;
                var timeOfLastClientActivityTicks =
                    *(long*)config.Read(Schema.SubscriptionTable.TimeOfLastActivityIndex, out tempSize);

                var clientAliveNotificationTicks = existingOptions.ClientAliveNotificationInterval;

                if (timeSinceBatchSentTicks > acknowledgementTimeoutTicks &&
                    now - timeOfLastClientActivityTicks > clientAliveNotificationTicks * 3)
                {
                    // last connected client exceeded ACK timeout and didn't send at least two 'client-alive' notifications - let the requesting client to open it
                    ForceReleaseAndOpenForNewClient(id, connectionOptions, tx);
                    tx.Commit();
                    return;
                }

                switch (connectionOptions.Strategy)
                {
                    case SubscriptionOpeningStrategy.TakeOver:
                        var existingOptionsConnectionStrategy = existingOptions.Strategy;

                        if (existingOptionsConnectionStrategy != SubscriptionOpeningStrategy.ForceAndKeep)
                        {
                            ForceReleaseAndOpenForNewClient(id, connectionOptions, tx);
                            tx.Commit();
                            return;
                        }
                        break;
                    case SubscriptionOpeningStrategy.ForceAndKeep:
                        ForceReleaseAndOpenForNewClient(id, connectionOptions, tx);
                        tx.Commit();
                        return;
                }
                throw new SubscriptionInUseException(
                    "Subscription is already in use. There can be only a single open subscription connection per subscription.");
            }
        }

        public void ReleaseSubscription(long id, bool forced = false)
        {
            SubscriptionConnectionOptions options;
            _openSubscriptions.TryRemove(id, out options);

            if (forced && options != null)
            {
                _forciblyReleasedSubscriptions.GetOrAdd(id,
                    new SizeLimitedConcurrentSet<string>(50, StringComparer.OrdinalIgnoreCase)).Add(options.ConnectionId);
            }
        }


        public unsafe void AcknowledgeBatchProcessed(long id, long lastEtag)
        {
            using (var tx = _environment.WriteTransaction())
            {
                var config = GetSubscriptionConfig(id, tx);
                var options = GetSubscriptionOptions(id);

                var acknoledgementTimeout = options.AcknowledgmentTimeout;

                var tempSize = 0;
                var timeSinceBatchSent = SystemTime.UtcNow.Ticks -
                                         *
                                             (long*)
                                                 config.Read(Schema.SubscriptionTable.TimeOfSendingLastBatch,
                                                     out tempSize); //config.TimeOfSendingLastBatch;
                if (timeSinceBatchSent > acknoledgementTimeout)
                    throw new TimeoutException(
                        "The subscription cannot be acknowledged because the timeout has been reached.");

                var subscriptionId = Bits.SwapBytes((ulong)id); ;

                int oldCriteriaSize;
                int longSizeForOutput;
                var now = SystemTime.UtcNow.Ticks;
                var tvb = new TableValueBuilder
                {
                    {(byte*)&subscriptionId, sizeof (long)},
                    {config.Read(Schema.SubscriptionTable.CriteriaIndex, out oldCriteriaSize), oldCriteriaSize},
                    {(byte*)&lastEtag, sizeof (long)},
                    {config.Read(Schema.SubscriptionTable.TimeOfSendingLastBatch, out longSizeForOutput), sizeof (long)},
                    {(byte*)&now, sizeof (long)}
                };
                var table = new Table(_subscriptionsSchema, Schema.SubsTree, tx);
                var existingSubscription = table.ReadByKey(new Slice((byte*)&subscriptionId, sizeof(long)));
                table.Update(existingSubscription.Id, tvb);
                tx.Commit();
            }
        }

        public void AssertOpenSubscriptionConnection(long id, string connection)
        {
            SubscriptionConnectionOptions options;
            if (_openSubscriptions.TryGetValue(id, out options) == false)
                throw new SubscriptionClosedException("There is no subscription with id: " + id + " being opened");

            if (options.ConnectionId.Equals(connection, StringComparison.OrdinalIgnoreCase) == false)
            {
                // prevent from concurrent work of multiple clients against the same subscription
                throw new SubscriptionInUseException("Subscription is being opened for a different connection.");
            }
        }

        public SubscriptionConnectionOptions GetSubscriptionOptions(long id)
        {
            SubscriptionConnectionOptions options;
            if (_openSubscriptions.TryGetValue(id, out options) == false)
                throw new SubscriptionClosedException("There is no open subscription with id: " + id);

            return options;
        }

        public unsafe SubscriptionCriteria GetCriteria(long id, DocumentsOperationContext context)
        {
            using (var tx = _environment.ReadTransaction())
            {
                var table = new Table(_subscriptionsSchema, Schema.SubsTree, tx);
                var subscriptionId = Bits.SwapBytes((ulong)id);

                var config = table.ReadByKey(new Slice((byte*)&subscriptionId, sizeof(long)));

                if (config == null)
                    throw new SubscriptionDoesNotExistException(
                        "There is no subscription configuration for specified identifier (id: " + id + ")");

                int criteriaSize;
                var criteriaPtr = config.Read(Schema.SubscriptionTable.CriteriaIndex, out criteriaSize);
                var criteriaBlittable = new BlittableJsonReaderObject(criteriaPtr, criteriaSize, context);
                return _subscriptionCriteriaDeserializer(criteriaBlittable);
            }
        }

        public unsafe TableValueReader GetSubscriptionConfig(long id, Transaction tx = null)
        {
            var localTx = tx ?? _environment.ReadTransaction();
            try
            {
                var table = new Table(_subscriptionsSchema, Schema.SubsTree, localTx);
                var subscriptionId = Bits.SwapBytes((ulong)id);

                var config = table.ReadByKey(new Slice((byte*)&subscriptionId, sizeof(long)));

                if (config == null)
                    throw new SubscriptionDoesNotExistException(
                        "There is no subscription configuration for specified identifier (id: " + id + ")");
                return config;
            }
            finally
            {
                if (tx == null)
                {
                    localTx.Commit();
                    localTx.Dispose();
                }
            }
        }

        public unsafe void AssertSubscriptionConfigExists(long id)
        {
            using (var tx = _environment.ReadTransaction())
            {
                var table = new Table(_subscriptionsSchema, Schema.SubsTree, tx);
                var subscriptionId = Bits.SwapBytes((ulong)id);

                if (table.VerifyKeyExists(new Slice((byte*)&subscriptionId, sizeof(long))) == false)
                    throw new SubscriptionDoesNotExistException(
                        "There is no subscription configuration for specified identifier (id: " + id + ")");
            }
        }

        public unsafe void DeleteSubscription(long id)
        {
            using (var tx = _environment.WriteTransaction())
            {
                var table = new Table(_subscriptionsSchema, Schema.SubsTree, tx);
                var subscriptionId = id;
                var subscription = table.ReadByKey(new Slice((byte*)&subscriptionId, sizeof(long)));
                table.Delete(subscription.Id);

                SizeLimitedConcurrentSet<string> temp;
                _forciblyReleasedSubscriptions.TryRemove(id, out temp);
                SubscriptionConnectionOptions subscriptionConnectionOptions;
                _openSubscriptions.TryRemove(id, out subscriptionConnectionOptions);

                tx.Commit();
            }
        }

        public List<TableValueReader> GetSubscriptions(int start, int take)
        {
            var subscriptions = new List<TableValueReader>();

            using (var tx = _environment.WriteTransaction())
            {
                var table = new Table(_subscriptionsSchema, Schema.SubsTree, tx);
                var seen = 0;
                var taken = 0;
                foreach (var subscriptionForKey in table.SeekByPrimaryKey(Slice.BeforeAllKeys))
                {
                    if (seen < start)
                    {
                        seen++;
                        continue;
                    }

                    subscriptions.Add(subscriptionForKey);

                    if (taken > take)
                        break;
                }
                return subscriptions;
            }
        }

        public unsafe List<SubscriptionConnectionOptions> GetDebugInfo()
        {
            using (var tx = _environment.ReadTransaction())
            {
                var subscriptions = new List<SubscriptionConnectionOptions>();

                var table = new Table(_subscriptionsSchema, Schema.SubsTree, tx);

                foreach (var subscriptionsForKey in table.SeekForwardFrom(_subscriptionsSchema.Key, Slice.BeforeAllKeys))
                {
                    foreach (var subscriptionForKey in subscriptionsForKey.Results)
                    {
                        int longSize;
                        var subscriptionId = *(long*)subscriptionForKey.Read(0, out longSize);
                        SubscriptionConnectionOptions options;
                        _openSubscriptions.TryGetValue(subscriptionId, out options);
                        subscriptions.Add(options);
                    }
                }

                return subscriptions;
            }
        }

        public unsafe void WriteSubscriptionTableValues(BlittableJsonTextWriter writer,
            DocumentsOperationContext context, List<TableValueReader> subscriptions)
        {
            writer.WriteStartArray();
            for (var i = 0; i < subscriptions.Count; i++)
            {
                var tvr = subscriptions[i];
                writer.WriteStartObject();
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

                writer.WritePropertyName(context.GetLazyStringForFieldWithCaching("SubscriptionId"));
                writer.WriteInteger(subscriptionId);
                writer.WriteComma();
                writer.WritePropertyName(context.GetLazyStringForFieldWithCaching("Criteria"));
                context.Write(writer, criteria);
                writer.WriteComma();
                writer.WritePropertyName(context.GetLazyStringForFieldWithCaching("AckEtag"));
                writer.WriteInteger(ackEtag);
                writer.WriteComma();
                writer.WritePropertyName(context.GetLazyStringForFieldWithCaching("TimeOfSendingLastBatch"));
                writer.WriteInteger(timeOfSendingLastBatch);
                writer.WriteComma();
                writer.WritePropertyName(context.GetLazyStringForFieldWithCaching("TimeOfLastClientActivity"));
                writer.WriteInteger(timeOfLastClientActivity);
                writer.WriteEndObject();

                if (i != subscriptions.Count - 1)
                    writer.WriteComma();
            }
            writer.WriteEndArray();
        }

        // ReSharper disable once ClassNeverInstantiated.Local
        public class Schema
        {
            public static readonly string IdsTree = "SubscriptionsIDs";
            public static readonly string SubsTree = "Subscriptions";
            public static readonly Slice Id = "Id";

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
}