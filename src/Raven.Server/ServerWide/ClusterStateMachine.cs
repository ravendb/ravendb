using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Security;
using Raven.Client.Http.OAuth;
using Raven.Client.Server;
using Raven.Client.Server.Operations.ApiKeys;
using Raven.Client.Server.Tcp;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Commands.Transformers;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using Voron;
using Voron.Data;
using Voron.Data.Tables;
using Voron.Exceptions;
using Voron.Impl;

namespace Raven.Server.ServerWide
{
    public class ClusterStateMachine : RachisStateMachine
    {
        private static readonly TableSchema ItemsSchema;
        private static readonly Slice EtagIndexName;
        private static readonly Slice Items;

        static ClusterStateMachine()
        {
            Slice.From(StorageEnvironment.LabelsContext, "Items", out Items);
            Slice.From(StorageEnvironment.LabelsContext, "EtagIndexName", out EtagIndexName);

            ItemsSchema = new TableSchema();

            // We use the follow format for the items data
            // { lowered key, key, data, etag }
            ItemsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1
            });

            ItemsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                Name = EtagIndexName,
                IsGlobal = true,
                StartIndex = 3
            });
        }

        public event EventHandler<(string dbName, long index, string type)> DatabaseChanged;

        public event EventHandler<(string dbName, long index, string type)> DatabaseValueChanged;

        protected override void Apply(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            if (cmd.TryGet("Type", out string type) == false)
                return;

            switch (type)
            {
                //The reason we have a separate case for removing node from database is because we must 
                //actually delete the database before we notify about changes to the record otherwise we 
                //don't know that it was us who needed to delete the database.
                case nameof(RemoveNodeFromDatabaseCommand):
                    RemoveNodeFromDatabase(context, cmd, index, leader);
                    break;

                case nameof(DeleteValueCommand):
                    DeleteValue(context, cmd, index, leader);
                    break;
                case nameof(IncrementClusterIdentityCommand):
                    var updatedDatabaseRecord = UpdateDatabase(context, type, cmd, index, leader);
                    if (!cmd.TryGet(nameof(IncrementClusterIdentityCommand.Prefix), out string prefix))
                    {
                        NotifyLeaderAboutError(index, leader,
                            new InvalidDataException($"Expected to find {nameof(IncrementClusterIdentityCommand)}.{nameof(IncrementClusterIdentityCommand.Prefix)} property in the Raft command but didn't find it..."));
                        return;
                    }

                    leader?.SetStateOf(index, updatedDatabaseRecord.Identities[prefix]);
                    break;
                case nameof(PutIndexCommand):
                case nameof(PutAutoIndexCommand):
                case nameof(DeleteIndexCommand):
                case nameof(SetIndexLockCommand):
                case nameof(SetIndexPriorityCommand):
                case nameof(PutTransformerCommand):
                case nameof(SetTransformerLockCommand):
                case nameof(DeleteTransformerCommand):
                case nameof(RenameTransformerCommand):
                case nameof(EditVersioningCommand):
                case nameof(UpdatePeriodicBackupCommand):
                case nameof(DeletePeriodicBackupCommand):
                case nameof(EditExpirationCommand):
                case nameof(ModifyDatabaseWatchersCommand):
                case nameof(ModifyConflictSolverCommand):
                case nameof(UpdateTopologyCommand):
                case nameof(DeleteDatabaseCommand):
                case nameof(ModifyCustomFunctionsCommand):
                case nameof(UpdateDatabaseWatcherCommand):
                case nameof(AddRavenEtlCommand):
                case nameof(AddSqlEtlCommand):
                case nameof(UpdateRavenEtlCommand):
                case nameof(UpdateSqlEtlCommand):
                case nameof(DeleteEtlCommand):
                    UpdateDatabase(context, type, cmd, index, leader);
                    break;
                case nameof(UpdatePeriodicBackupStatusCommand):
                case nameof(AcknowledgeSubscriptionBatchCommand):
                case nameof(CreateSubscriptionCommand):
                case nameof(DeleteSubscriptionCommand):
                case nameof(StoreEtlStatusCommand):
                    SetValueForTypedDatabaseCommand(context, type, cmd, index, leader);
                    break;
                case nameof(PutApiKeyCommand):
                    PutValue<ApiKeyDefinition>(context, cmd, index, leader);
                    break;
                case nameof(AddDatabaseCommand):
                    AddDatabase(context, cmd, index, leader);
                    break;
            }
        }

        private unsafe void SetValueForTypedDatabaseCommand(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            UpdateValueForDatabaseCommand updateCommand = null;
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

                updateCommand = (UpdateValueForDatabaseCommand)JsonDeserializationCluster.Commands[type](cmd);

                var record = ReadDatabase(context, updateCommand.DatabaseName);
                if (record == null)
                {
                    NotifyLeaderAboutError(index, leader, new CommandExecutionException($"Cannot set typed value of type {type} for database {updateCommand.DatabaseName}, because does not exist"));
                    return;
                }

                BlittableJsonReaderObject itemBlittable = null;

                var itemKey = updateCommand.GetItemId();
                using (Slice.From(context.Allocator, itemKey.ToLowerInvariant(), out Slice valueNameLowered))
                {
                    if (items.ReadByKey(valueNameLowered, out TableValueReader reader))
                    {
                        var ptr = reader.Read(2, out int size);
                        itemBlittable = new BlittableJsonReaderObject(ptr, size, context);
                    }

                    try
                    {
                        itemBlittable = updateCommand.GetUpdatedValue(index, record, context, itemBlittable);

                        // if returned null, means, there is nothing to update and we just wanted to delete the value
                        if (itemBlittable == null)
                        {
                            items.DeleteByKey(valueNameLowered);
                            return;
                        }

                        // here we get the item key again, in case it was changed (a new entity, etc)
                        itemKey = updateCommand.GetItemId();
                    }
                    catch (Exception e)
                    {
                        NotifyLeaderAboutError(index, leader,
                            new CommandExecutionException($"Cannot set typed value of type {type} for database {updateCommand.DatabaseName}, because does not exist", e));
                        return;
                    }
                }

                using (Slice.From(context.Allocator, itemKey, out Slice valueName))
                using (Slice.From(context.Allocator, itemKey.ToLowerInvariant(), out Slice valueNameLowered))
                {
                    UpdateValue(index, items, valueNameLowered, valueName, itemBlittable);
                }
            }
            finally
            {
                NotifyDatabaseValueChanged(context, updateCommand?.DatabaseName, index, type);
            }
        }

        private readonly RachisLogIndexNotifications _rachisLogIndexNotifications = new RachisLogIndexNotifications(CancellationToken.None);

        public async Task WaitForIndexNotification(long index)
        {
            await _rachisLogIndexNotifications.WaitForIndexNotification(index, _parent.OperationTimeout);
        }

        private unsafe void RemoveNodeFromDatabase(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            var remove = JsonDeserializationCluster.RemoveNodeFromDatabaseCommand(cmd);
            var databaseName = remove.DatabaseName;
            var databaseNameLowered = databaseName.ToLowerInvariant();
            using (Slice.From(context.Allocator, "db/" + databaseNameLowered, out Slice lowerKey))
            using (Slice.From(context.Allocator, "db/" + databaseName, out Slice key))
            {
                if (items.ReadByKey(lowerKey, out TableValueReader reader) == false)
                {
                    NotifyLeaderAboutError(index, leader, new InvalidOperationException($"The database {databaseName} does not exists"));
                    return;
                }
                var doc = new BlittableJsonReaderObject(reader.Read(2, out int size), size, context);

                var databaseRecord = JsonDeserializationCluster.DatabaseRecord(doc);

                if (doc.TryGet(nameof(DatabaseRecord.Topology), out BlittableJsonReaderObject topology) == false)
                {
                    items.DeleteByKey(lowerKey);
                    NotifyDatabaseChanged(context, databaseName, index, nameof(RemoveNodeFromDatabaseCommand));
                    return;
                }
                remove.UpdateDatabaseRecord(databaseRecord, index);

                if (databaseRecord.Topology.Members.Count == 0 &&
                    databaseRecord.Topology.Promotables.Count == 0 &&
                    databaseRecord.Topology.Watchers.Count == 0)
                {
                    // delete database record
                    items.DeleteByKey(lowerKey);

                    // delete all values linked to database record - for subscription, etl etc.
                    CleanupDatabaseRelatedValues(context, items, databaseName);

                    items.DeleteByPrimaryKeyPrefix(lowerKey);
                    NotifyDatabaseChanged(context, databaseName, index, nameof(RemoveNodeFromDatabaseCommand));
                    return;
                }

                var updated = EntityToBlittable.ConvertEntityToBlittable(databaseRecord, DocumentConventions.Default, context);

                UpdateValue(index, items, lowerKey, key, updated);

                NotifyDatabaseChanged(context, databaseName, index, nameof(RemoveNodeFromDatabaseCommand));
            }
        }

        private void CleanupDatabaseRelatedValues(TransactionOperationContext context, Table items, string dbNameLowered)
        {
            var subscriptionItemsPrefix = SubscriptionState.GenerateSubscriptionPrefix(dbNameLowered).ToLowerInvariant();
            using (Slice.From(context.Allocator, subscriptionItemsPrefix, out Slice loweredKey))
            {

                items.DeleteByPrimaryKeyPrefix(loweredKey);
            }
        }

        private static unsafe void UpdateValue(long index, Table items, Slice lowerKey, Slice key, BlittableJsonReaderObject updated)
        {
            using (items.Allocate(out TableValueBuilder builder))
            {
                builder.Add(lowerKey);
                builder.Add(key);
                builder.Add(updated.BasePointer, updated.Size);
                builder.Add(Bits.SwapBytes(index));

                items.Set(builder);
            }
        }

        private unsafe void AddDatabase(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            var addDatabaseCommand = JsonDeserializationCluster.AddDatabaseCommand(cmd);
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                using (Slice.From(context.Allocator, "db/" + addDatabaseCommand.Name, out Slice valueName))
                using (Slice.From(context.Allocator, "db/" + addDatabaseCommand.Name.ToLowerInvariant(), out Slice valueNameLowered))
                using (var databaseRecordAsJson = EntityToBlittable.ConvertEntityToBlittable(addDatabaseCommand.Record, DocumentConventions.Default, context))
                {
                    if (addDatabaseCommand.Etag != null)
                    {
                        if (items.ReadByKey(valueNameLowered, out TableValueReader reader) == false && addDatabaseCommand.Etag != 0)
                        {
                            NotifyLeaderAboutError(index, leader, new ConcurrencyException("Concurrency violation, the database " + addDatabaseCommand.Name + " does not exists, but had a non zero etag"));
                            return;
                        }

                        var actualEtag = Bits.SwapBytes(*(long*)reader.Read(3, out int size));
                        Debug.Assert(size == sizeof(long));

                        if (actualEtag != addDatabaseCommand.Etag.Value)
                        {
                            NotifyLeaderAboutError(index, leader,
                                new ConcurrencyException("Concurrency violation, the database " + addDatabaseCommand.Name + " has etag " + actualEtag + " but was expecting " + addDatabaseCommand.Etag));
                            return;
                        }
                    }

                    UpdateValue(index, items, valueNameLowered, valueName, databaseRecordAsJson);
                }
            }
            finally
            {
                NotifyDatabaseChanged(context, addDatabaseCommand.Name, index, nameof(AddDatabaseCommand));
            }
        }

        private void DeleteValue(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                var delCmd = JsonDeserializationCluster.DeleteValueCommand(cmd);
                if (delCmd.Name.StartsWith("db/"))
                {
                    NotifyLeaderAboutError(index, leader, new InvalidOperationException("Cannot set " + delCmd.Name + " using DeleteValueCommand, only via dedicated Database calls"));
                    return;
                }
                using (Slice.From(context.Allocator, delCmd.Name, out Slice str))
                {
                    items.DeleteByKey(str);
                }
            }
            finally
            {
                NotifyIndexProcessed(context, index);
            }
        }

        private void PutValue<T>(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                var putVal = (PutValueCommand<T>)CommandBase.CreateFrom(cmd);
                if (putVal.Name.StartsWith("db/"))
                {
                    NotifyLeaderAboutError(index, leader, new InvalidOperationException("Cannot set " + putVal.Name + " using PutValueCommand, only via dedicated Database calls"));
                    return;
                }

                using (Slice.From(context.Allocator, putVal.Name, out Slice valueName))
                using (Slice.From(context.Allocator, putVal.Name.ToLowerInvariant(), out Slice valueNameLowered))
                using (var rec = context.ReadObject(putVal.ValueToJson(), "inner-val"))
                {
                    UpdateValue(index, items, valueNameLowered, valueName, rec);
                }
            }
            finally
            {
                NotifyIndexProcessed(context, index);
            }
        }

        private void NotifyIndexProcessed(TransactionOperationContext context, long index)
        {
            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += transaction =>
            {
                if (transaction is LowLevelTransaction llt && llt.Committed)
                    _rachisLogIndexNotifications.NotifyListenersAbout(index);
            };
        }

        private void NotifyDatabaseChanged(TransactionOperationContext context, string databaseName, long index, string type)
        {
            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += transaction =>
            {
                if (transaction is LowLevelTransaction llt && llt.Committed)
                    TaskExecutor.Execute(_ =>
                    {
                        try
                        {
                            DatabaseChanged?.Invoke(this, (databaseName, index, type));
                        }
                        finally
                        {
                            _rachisLogIndexNotifications.NotifyListenersAbout(index);
                        }
                    }, null);
            };
        }

        private void NotifyDatabaseValueChanged(TransactionOperationContext context, string databaseName, long index, string type)
        {
            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += transaction =>
            {
                if (transaction is LowLevelTransaction llt && llt.Committed)
                    TaskExecutor.Execute(_ =>
                    {
                        try
                        {
                            DatabaseValueChanged?.Invoke(this, (databaseName, index, type));
                        }
                        finally
                        {
                            _rachisLogIndexNotifications.NotifyListenersAbout(index);
                        }
                    }, null);
            };
        }

        private static readonly StringSegment DatabaseName = new StringSegment("DatabaseName");

        private DatabaseRecord UpdateDatabase(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            if (cmd.TryGet(DatabaseName, out string databaseName) == false)
                throw new ArgumentException("Update database command must contain a DatabaseName property");

            DatabaseRecord databaseRecord;
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                var dbKey = "db/" + databaseName;

                using (Slice.From(context.Allocator, dbKey, out Slice valueName))
                using (Slice.From(context.Allocator, dbKey.ToLowerInvariant(), out Slice valueNameLowered))
                {
                    var databaseRecordJson = ReadInternal(context, out long etag, valueNameLowered);

                    if (databaseRecordJson == null)
                    {
                        NotifyLeaderAboutError(index, leader, new DatabaseDoesNotExistException($"Cannot execute update command of type {type} for {databaseName} because it does not exists"));
                        return null;
                    }

                    databaseRecord = JsonDeserializationCluster.DatabaseRecord(databaseRecordJson);
                    var updateCommand = (UpdateDatabaseCommand)JsonDeserializationCluster.Commands[type](cmd);

                    if (updateCommand.Etag != null && etag != updateCommand.Etag.Value)
                    {
                        NotifyLeaderAboutError(index, leader,
                            new ConcurrencyException($"Concurrency violation at executing {type} command, the database {databaseRecord.DatabaseName} has etag {etag} but was expecting {updateCommand.Etag}"));
                        return null;
                    }

                    try
                    {
                        var relatedRecordIdToDelete = updateCommand.UpdateDatabaseRecord(databaseRecord, index);
                        if (relatedRecordIdToDelete != null)
                        {
                            var itemKey = relatedRecordIdToDelete;
                            using (Slice.From(context.Allocator, itemKey, out Slice _))
                            using (Slice.From(context.Allocator, itemKey.ToLowerInvariant(), out Slice valueNameToDeleteLowered))
                            {
                                items.DeleteByKey(valueNameToDeleteLowered);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        NotifyLeaderAboutError(index, leader, new CommandExecutionException($"Cannot execute command of type {type} for database {databaseName}", e));
                        return null;
                    }

                    var updatedDatabaseBlittable = EntityToBlittable.ConvertEntityToBlittable(databaseRecord, DocumentConventions.Default, context);

                    UpdateValue(index, items, valueNameLowered, valueName, updatedDatabaseBlittable);
                }
            }
            finally
            {
                NotifyDatabaseChanged(context, databaseName, index, type);
            }

            return databaseRecord;
        }

        private static void NotifyLeaderAboutError(long index, Leader leader, Exception e)
        {
            // ReSharper disable once UseNullPropagation
            if (leader == null)
                return;

            leader.SetStateOf(index, tcs =>
            {
                tcs.TrySetException(e);
            });
        }

        public override bool ShouldSnapshot(Slice slice, RootObjectType type)
        {
            return slice.Content.Equals(Items.Content);
        }

        public override void Initialize(RachisConsensus parent, TransactionOperationContext context)
        {
            base.Initialize(parent, context);
            ItemsSchema.Create(context.Transaction.InnerTransaction, Items, 32);
        }

        public IEnumerable<Tuple<string, BlittableJsonReaderObject>> ItemsStartingWith(TransactionOperationContext context, string prefix, int start, int take)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            var dbKey = prefix.ToLowerInvariant();
            using (Slice.From(context.Allocator, dbKey, out Slice loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, start))
                {
                    if (take-- <= 0)
                        yield break;

                    yield return GetCurrentItem(context, result);
                }
            }
        }

        public IEnumerable<string> GetDatabaseNames(TransactionOperationContext context, int start = 0, int take = int.MaxValue)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            var dbKey = "db/";
            using (Slice.From(context.Allocator, dbKey, out Slice loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
                {
                    if (take-- <= 0)
                        yield break;

                    yield return GetCurrentItemKey(context, result).Substring(3);
                }
            }
        }

        private static unsafe string GetCurrentItemKey(TransactionOperationContext context, Table.TableValueHolder result)
        {
            return Encoding.UTF8.GetString(result.Reader.Read(1, out int size), size);
        }

        private static unsafe Tuple<string, BlittableJsonReaderObject> GetCurrentItem(TransactionOperationContext context, Table.TableValueHolder result)
        {
            var ptr = result.Reader.Read(2, out int size);
            var doc = new BlittableJsonReaderObject(ptr, size, context);

            var key = Encoding.UTF8.GetString(result.Reader.Read(1, out size), size);

            return Tuple.Create(key, doc);
        }

        public DatabaseRecord ReadDatabase(TransactionOperationContext context, string name)
        {
            return ReadDatabase(context, name, out long etag);
        }

        public DatabaseRecord ReadDatabase(TransactionOperationContext context, string name, out long etag)
        {
            var doc = Read(context, "db/" + name.ToLowerInvariant(), out etag);
            if (doc == null)
                return null;
            return JsonDeserializationCluster.DatabaseRecord(doc);
        }
        public BlittableJsonReaderObject Read(TransactionOperationContext context, string name)
        {
            return Read(context, name, out long etag);
        }

        public BlittableJsonReaderObject Read(TransactionOperationContext context, string name, out long etag)
        {

            var dbKey = name.ToLowerInvariant();
            using (Slice.From(context.Allocator, dbKey, out Slice key))
            {
                return ReadInternal(context, out etag, key);
            }
        }

        private static unsafe BlittableJsonReaderObject ReadInternal(TransactionOperationContext context, out long etag, Slice key)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            if (items.ReadByKey(key, out TableValueReader reader) == false)
            {
                etag = 0;
                return null;
            }

            var ptr = reader.Read(2, out int size);
            var doc = new BlittableJsonReaderObject(ptr, size, context);

            etag = Bits.SwapBytes(*(long*)reader.Read(3, out size));
            Debug.Assert(size == sizeof(long));

            return doc;
        }

        public static IEnumerable<(long, BlittableJsonReaderObject)> ReadValuesStartingWith(TransactionOperationContext context, string startsWithKey)
        {
            var startsWithKeyLower = startsWithKey.ToLowerInvariant();
            using (Slice.From(context.Allocator, startsWithKeyLower, out Slice startsWithSlice))
            {
                return ReadValuesStartingWith(context, startsWithSlice);
            }
        }

        public static IEnumerable<(long, BlittableJsonReaderObject)> ReadValuesStartingWith(TransactionOperationContext context, Slice startsWithKey)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            foreach (var holder in items.SeekByPrimaryKeyPrefix(startsWithKey, Slices.Empty, 0))
            {
                var reader = holder.Reader;
                int size = GetDataAndEtagTupleFromReader(context, reader, out BlittableJsonReaderObject doc, out long etag);
                Debug.Assert(size == sizeof(long));

                yield return (etag, doc);
            }

        }

        private static unsafe int GetDataAndEtagTupleFromReader(TransactionOperationContext context, TableValueReader reader, out BlittableJsonReaderObject doc, out long etag)
        {
            var ptr = reader.Read(2, out int size);
            doc = new BlittableJsonReaderObject(ptr, size, context);

            etag = Bits.SwapBytes(*(long*)reader.Read(3, out size));
            Debug.Assert(size == sizeof(long));
            return size;
        }

        public override async Task<Stream> ConnectToPeer(string url, string apiKey)
        {
            var info = await ReplicationUtils.GetTcpInfoAsync(url, "Rachis.Server", apiKey, "Cluster");
            var authenticator = new ApiKeyAuthenticator();

            var tcpInfo = new Uri(info.Url);
            var tcpClient = new TcpClient();
            Stream stream = null;
            try
            {
                await tcpClient.ConnectAsync(tcpInfo.Host, tcpInfo.Port);
                stream = await TcpUtils.WrapStreamWithSslAsync(tcpClient, info);

                using (ContextPoolForReadOnlyOperations.AllocateOperationContext(out JsonOperationContext context))
                {
                    var apiToken = await authenticator.GetAuthenticationTokenAsync(apiKey, url, context);
                    var msg = new DynamicJsonValue
                    {
                        [nameof(TcpConnectionHeaderMessage.DatabaseName)] = null,
                        [nameof(TcpConnectionHeaderMessage.Operation)] = TcpConnectionHeaderMessage.OperationTypes.Cluster,
                        [nameof(TcpConnectionHeaderMessage.AuthorizationToken)] = apiToken,
                    };
                    using (var writer = new BlittableJsonTextWriter(context, stream))
                    using (var msgJson = context.ReadObject(msg, "message"))
                    {
                        context.Write(writer, msgJson);
                    }
                    using (var response = context.ReadForMemory(stream, "cluster-ConnectToPeer-header-response"))
                    {

                        var reply = JsonDeserializationServer.TcpConnectionHeaderResponse(response);
                        switch (reply.Status)
                        {
                            case TcpConnectionHeaderResponse.AuthorizationStatus.Forbidden:
                                throw AuthorizationException.Forbidden("Server");
                            case TcpConnectionHeaderResponse.AuthorizationStatus.Success:
                                break;
                            default:
                                throw AuthorizationException.Unauthorized(reply.Status, "Server");
                        }
                    }
                }
                return stream;
            }
            catch (Exception)
            {
                stream?.Dispose();
                tcpClient.Dispose();
                throw;
            }
        }

        public override void OnSnapshotInstalled(TransactionOperationContext context, long lastIncludedIndex)
        {
            var listOfDatabaseName = GetDatabaseNames(context).ToList();
            //There is potentially a lot of work to be done here so we are responding to the change on a separate task.
            var onDatabaseChanged = DatabaseChanged;
            if (onDatabaseChanged != null)
            {
                _rachisLogIndexNotifications.NotifyListenersAbout(lastIncludedIndex);
                TaskExecutor.Execute(_ =>
                {
                    foreach (var db in listOfDatabaseName)
                        onDatabaseChanged.Invoke(this, (db, lastIncludedIndex, "SnapshotInstalled"));
                }, null);
            }
        }
    }

    public class RachisLogIndexNotifications
    {
        private readonly CancellationToken _token;
        private Holder _lastModifiedIndex = new Holder();
        private readonly AsyncManualResetEvent _notifiedListeners;

        private class Holder
        {
            public long Val;
            public string CachedToString;
        }

        public RachisLogIndexNotifications(CancellationToken token)
        {
            _token = token;
            _notifiedListeners = new AsyncManualResetEvent(token);
        }

        public bool IsMatch(string etag)
        {
            var copy = _lastModifiedIndex;
            if (copy.CachedToString == null)
                copy.CachedToString = copy.Val.ToString();
            return etag == copy.CachedToString;
        }

        public async Task WaitForIndexNotification(long index, TimeSpan? timeoutInMs = null)
        {
            Task timeoutTask = null;
            if (timeoutInMs.HasValue)
                timeoutTask = TimeoutManager.WaitFor(timeoutInMs.Value, _token);
            while (index > Volatile.Read(ref _lastModifiedIndex.Val) &&
                    (timeoutInMs.HasValue == false || timeoutTask.IsCompleted == false))
            {
                var task = _notifiedListeners.WaitAsync();
                if (timeoutInMs.HasValue == false)
                {
                    await task;
                }
                else if (timeoutTask == await Task.WhenAny(task, timeoutTask))
                {
                    ThrowTimeoutException(timeoutInMs.Value, index, _lastModifiedIndex.Val);
                }
            }
        }

        private static void ThrowTimeoutException(TimeSpan value, long index, long lastModifiedIndex)
        {
            throw new TimeoutException($"Waited for {value} but didn't get index notification for {index}. " +
                                       $"Last commit index is: {lastModifiedIndex}.");
        }

        public void NotifyListenersAbout(long index)
        {
            var lastModifed = _lastModifiedIndex;
            var holder = new Holder
            {
                Val = index
            };
            while (index > lastModifed.Val)
            {
                lastModifed = Interlocked.CompareExchange(ref _lastModifiedIndex, holder, lastModifed);
            }
            _notifiedListeners.SetAndResetAtomically();
        }
    }
}