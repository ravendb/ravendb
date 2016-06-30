using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
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
using Sparrow;
//using SubscriptionTable = Raven.Server.Documents.SubscriptionStorage.Schema.SubscriptionTable;

namespace Raven.Server.Documents
{

    // todo: implement functionality for limiting amount of opened subscriptions
    // todo: implement functionality for removing "old" subscriptions using dedicated index
    public class SubscriptionStorage : IDisposable
    {
        public static TimeSpan TwoMinutesTimespan = TimeSpan.FromMinutes(2);
        private readonly ConcurrentDictionary<long, SubscriptionConnectionState> _subscriptionConnectionStates = new ConcurrentDictionary<long, SubscriptionConnectionState>();
        private readonly TableSchema _subscriptionsSchema = new TableSchema();
        private readonly DocumentDatabase _db;
        private readonly StorageEnvironment _environment;
        private Logger _log; //todo: add logging

        private readonly UnmanagedBuffersPool _unmanagedBuffersPool;

        public SubscriptionStorage(DocumentDatabase db)
        {
            _db = db;
            //TODO: You aren't copying all the other details from the configuration
            var options = _db.Configuration.Core.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly()
                : StorageEnvironmentOptions.ForPath(Path.Combine(_db.Configuration.Core.DataDirectory, "Subscriptions"));
            
            _environment = new StorageEnvironment(options, db.LoggerSetup);
            _unmanagedBuffersPool = new UnmanagedBuffersPool($"Subscriptions");

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

        public SubscriptionConnectionState OpenSubscription(SubscriptionConnectionOptions options)
        {
            return _subscriptionConnectionStates.GetOrAdd(options.SubscriptionId,
                _ => new SubscriptionConnectionState(options));
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
            if (_subscriptionConnectionStates.ContainsKey(id) == false)
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

        public unsafe void AssertSubscriptionConfigExists(long id)
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
            SubscriptionConnectionState subscriptionConnectionState;
            if (_subscriptionConnectionStates.TryRemove(id, out subscriptionConnectionState))
            {
                subscriptionConnectionState.EndConnection();
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

        public List<TableValueReader> GetSubscriptions(int start, int take)
        {
            var subscriptions = new List<TableValueReader>();

            using (var tx = _environment.WriteTransaction())
            {
                var table = new Table(_subscriptionsSchema, Schema.SubsTree, tx);
                var seen = 0;
                var taken = 0;
                foreach (var subscriptionForKey in table.SeekByPrimaryKey(Slices.BeforeAllKeys))
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

                foreach (var subscriptionsForKey in table.SeekForwardFrom(_subscriptionsSchema.Key, Slices.BeforeAllKeys))
                {
                    foreach (var subscriptionForKey in subscriptionsForKey.Results)
                    {
                        int longSize;
                        var subscriptionId = *(long*)subscriptionForKey.Read(0, out longSize);
                        SubscriptionConnectionState connectionState;

                        if (_subscriptionConnectionStates.TryGetValue(subscriptionId, out connectionState))
                        {
                            var options = connectionState.Connection;
                            if (options != null)
                            {
                                subscriptions.Add(connectionState.Connection);
                            }
                        }
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
            public static readonly Slice Id = Slice.From( StorageEnvironment.LabelsContext, "Id", ByteStringType.Immutable);

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