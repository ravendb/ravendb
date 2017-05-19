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
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Security;
using Raven.Client.Extensions;
using Raven.Client.Http.OAuth;
using Raven.Client.Server;
using Raven.Client.Server.Tcp;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.Indexes;
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

        public event EventHandler<(string dbName, long index)> DatabaseChanged;

        protected override void Apply(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            string type;
            if (cmd.TryGet("Type", out type) == false)
                return;

            switch (type)
            {
                case nameof(DeleteDatabaseCommand):
                    DeleteDatabase(context, cmd, index, leader);
                    break;

                case nameof(RemoveNodeFromDatabaseCommand):
                    RemoveNodeFromDatabase(context, cmd, index, leader);
                    break;

                case nameof(DeleteValueCommand):
                    DeleteValue(context, cmd, index, leader);
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
                case nameof(EditPeriodicBackupCommand):
                case nameof(EditExpirationCommand):
                case nameof(ModifyDatabaseWatchersCommand):
                case nameof(ModifyConflictSolverCommand):
                case nameof(UpdateTopologyCommand):
                    UpdateDatabase(context, type, cmd, index, leader);
                    break;
                case nameof(AcknowledgeSubscriptionBatchCommand):
                case nameof(CreateSubscriptionCommand):
                case nameof(DeleteSubscriptionCommand):
                    SetValueForTypedDatabaseCommand(context,type, cmd, index, leader);
                    break;
                case nameof(PutValueCommand):
                    PutValue(context, cmd, index, leader);
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

                updateCommand = JsonDeserializationCluster.UpdateValueCommands[type](cmd);
                
                var record = ReadDatabase(context, updateCommand.DatabaseName);
                if (record == null)
                {
                    NotifyLeaderAboutError(index, leader, new CommandExecutionException($"Cannot set typed value of type {type} for database {updateCommand.DatabaseName}, because does not exist"));
                    return;
                }

                DynamicJsonValue djv;
                BlittableJsonReaderObject existingValue = null;

                var itemKey = updateCommand.GetItemId();
                using (Slice.From(context.Allocator, itemKey, out Slice valueName))
                using (Slice.From(context.Allocator, itemKey.ToLowerInvariant(), out Slice valueNameLowered))
                {
                    if (items.ReadByKey(valueNameLowered, out TableValueReader reader))
                    {
                        var ptr = reader.Read(2, out int size);
                        existingValue = new BlittableJsonReaderObject(ptr, size, context);
                    }


                    try
                    {
                        djv = updateCommand.GetUpdatedValue(index, record, existingValue);

                        // if returned null, means, there is nothing to update and we just wanted to delete the value
                        if (djv == null)
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
                    if (existingValue==null)
                        existingValue = context.ReadObject(djv, updateCommand.GetItemId());

                    using (var rec = context.ReadObject(existingValue, "inner-val"))
                    {
                        UpdateDatabaseRecord(index, items, valueNameLowered, valueName, rec);
                    }
                   
                }
            }
            finally
            {
                NotifyDatabaseChanged(context, updateCommand?.DatabaseName, index);
            }
        }

        private readonly RachisLogIndexNotifications _rachisLogIndexNotifications = new RachisLogIndexNotifications(CancellationToken.None);
        public Task WaitForIndexNotification(long index)
        {
            return _rachisLogIndexNotifications.WaitForIndexNotification(index);
        }

        private unsafe void RemoveNodeFromDatabase(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            var remove = JsonDeserializationCluster.RemoveNodeFromDatabaseCommand(cmd);
            Slice loweredKey;
            Slice key;
            var databaseName = remove.DatabaseName;
            using (Slice.From(context.Allocator, "db/" + databaseName.ToLowerInvariant(), out loweredKey))
            using (Slice.From(context.Allocator, "db/" + databaseName, out key))
            {
                TableValueReader reader;
                if (items.ReadByKey(loweredKey, out reader) == false)
                {
                    NotifyLeaderAboutError(index, leader, new InvalidOperationException($"The database {databaseName} does not exists"));
                    return;
                }
                int size;
                var doc = new BlittableJsonReaderObject(reader.Read(2, out size), size, context);

                var databaseRecord = JsonDeserializationCluster.DatabaseRecord(doc);

                BlittableJsonReaderObject topology;
                if (doc.TryGet(nameof(DatabaseRecord.Topology), out topology) == false)
                {
                    items.DeleteByKey(loweredKey);
                    NotifyDatabaseChanged(context, databaseName, index);
                    return;
                }

                databaseRecord.Topology.Members.RemoveAll(m => m.NodeTag == remove.NodeTag);
                databaseRecord.Topology.Promotables.RemoveAll(p => p.NodeTag == remove.NodeTag);

                databaseRecord.DeletionInProgress.Remove(remove.NodeTag);

                if (databaseRecord.Topology.Members.Count == 0 &&
                    databaseRecord.Topology.Promotables.Count == 0 &&
                    databaseRecord.Topology.Watchers.Count == 0)
                {
                    items.DeleteByKey(loweredKey);
                    NotifyDatabaseChanged(context, databaseName, index);
                    return;
                }

                var updated = EntityToBlittable.ConvertEntityToBlittable(databaseRecord, DocumentConventions.Default, context);

                UpdateDatabaseRecord(index, items, loweredKey, key, updated);

                NotifyDatabaseChanged(context, databaseName, index);
            }
        }

        private static unsafe void UpdateDatabaseRecord(long index, Table items, Slice loweredKey, Slice key, BlittableJsonReaderObject updated)
        {
            TableValueBuilder builder;
            using (items.Allocate(out builder))
            {
                builder.Add(loweredKey);
                builder.Add(key);
                builder.Add(updated.BasePointer, updated.Size);
                builder.Add(Bits.SwapBytes(index));

                items.Set(builder);
            }
        }

        private unsafe void DeleteDatabase(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            var delDb = JsonDeserializationCluster.DeleteDatabaseCommand(cmd);
            Slice loweredKey;
            Slice key;
            var databaseName = delDb.DatabaseName;
            using (Slice.From(context.Allocator, "db/" + databaseName.ToLowerInvariant(), out loweredKey))
            using (Slice.From(context.Allocator, "db/" + databaseName, out key))
            {
                TableValueReader reader;
                if (items.ReadByKey(loweredKey, out reader) == false)
                {
                    NotifyLeaderAboutError(index, leader, new DatabaseDoesNotExistException($"The database {databaseName} does not exists, cannot delete it"));
                    return;
                }

                int size;
                var deletionInProgressStatus = delDb.HardDelete
                    ? DeletionInProgressStatus.HardDelete
                    : DeletionInProgressStatus.SoftDelete;
                var doc = new BlittableJsonReaderObject(reader.Read(2, out size), size, context);
                var databaseRecord = JsonDeserializationCluster.DatabaseRecord(doc);
                if (databaseRecord.DeletionInProgress == null)
                    databaseRecord.DeletionInProgress = new Dictionary<string, DeletionInProgressStatus>();

                if (string.IsNullOrEmpty(delDb.FromNode) == false)
                {
                    if (databaseRecord.Topology.RelevantFor(delDb.FromNode) == false)
                    {
                        NotifyLeaderAboutError(index, leader, new DatabaseDoesNotExistException($"The database {databaseName} does not exists on node {delDb.FromNode}"));
                        return;
                    }
                    databaseRecord.Topology.RemoveFromTopology(delDb.FromNode);

                    databaseRecord.DeletionInProgress[delDb.FromNode] = deletionInProgressStatus;
                }
                else
                {
                    var allNodes = databaseRecord.Topology.Members.Select(m => m.NodeTag)
                        .Concat(databaseRecord.Topology.Promotables.Select(p => p.NodeTag));

                    foreach (var node in allNodes)
                        databaseRecord.DeletionInProgress[node] = deletionInProgressStatus;

                    databaseRecord.Topology = new DatabaseTopology();
                }
                
                using (var updated = EntityToBlittable.ConvertEntityToBlittable(databaseRecord, DocumentConventions.Default, context))
                {
                    UpdateDatabaseRecord(index, items, loweredKey, key, updated);
                }

                NotifyDatabaseChanged(context, databaseName, index);
            }
        }

        private unsafe void AddDatabase(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            var addDatabaseCommand = JsonDeserializationCluster.AddDatabaseCommand(cmd);
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

                Slice valueName, valueNameLowered;
                using (Slice.From(context.Allocator, "db/" + addDatabaseCommand.Name, out valueName))
                using (Slice.From(context.Allocator, "db/" + addDatabaseCommand.Name.ToLowerInvariant(), out valueNameLowered))
                using (var rec = context.ReadObject(addDatabaseCommand.Value, "inner-val"))
                {
                    if (addDatabaseCommand.Etag != null)
                    {
                        TableValueReader reader;
                        if (items.ReadByKey(valueNameLowered, out reader) == false && addDatabaseCommand.Etag != 0)
                        {
                            NotifyLeaderAboutError(index, leader, new ConcurrencyException("Concurrency violation, the database " + addDatabaseCommand.Name + " does not exists, but had a non zero etag"));
                            return;
                        }

                        int size;
                        var actualEtag = Bits.SwapBytes(*(long*)reader.Read(3, out size));
                        Debug.Assert(size == sizeof(long));

                        if (actualEtag != addDatabaseCommand.Etag.Value)
                        {
                            NotifyLeaderAboutError(index, leader,
                                new ConcurrencyException("Concurrency violation, the database " + addDatabaseCommand.Name + " has etag " + actualEtag + " but was expecting " + addDatabaseCommand.Etag));
                            return;
                        }
                    }

                    UpdateDatabaseRecord(index, items, valueNameLowered, valueName, rec);
                }
            }
            finally
            {
                NotifyDatabaseChanged(context, addDatabaseCommand.Name, index);
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
                Slice str;
                using (Slice.From(context.Allocator, delCmd.Name, out str))
                {
                    items.DeleteByKey(str);
                }
            }
            finally
            {
                NotifyIndexProcessed(context, index);
            }
        }

        private void PutValue(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                var putVal = JsonDeserializationCluster.PutValueCommand(cmd);
                if (putVal.Name.StartsWith("db/"))
                {
                    NotifyLeaderAboutError(index, leader, new InvalidOperationException("Cannot set " + putVal.Name + " using PutValueCommand, only via dedicated Database calls"));
                    return;
                }

                Slice valueName, valueNameLowered;
                using (Slice.From(context.Allocator, putVal.Name, out valueName))
                using (Slice.From(context.Allocator, putVal.Name.ToLowerInvariant(), out valueNameLowered))
                using (var rec = context.ReadObject(putVal.Value, "inner-val"))
                {
                    UpdateDatabaseRecord(index, items, valueNameLowered, valueName, rec);
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

        private void NotifyDatabaseChanged(TransactionOperationContext context, string databaseName, long index)
        {
            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += transaction =>
            {
                if (transaction is LowLevelTransaction llt && llt.Committed)
                    TaskExecutor.Execute(_ =>
                    {
                        try
                        {
                            DatabaseChanged?.Invoke(this, (databaseName, index));
                        }
                        finally
                        {
                            _rachisLogIndexNotifications.NotifyListenersAbout(index);
                        }
                    }, null);
            };
        }

        private static readonly StringSegment DatabaseName = new StringSegment("DatabaseName");

        private void UpdateDatabase(TransactionOperationContext context, string type, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            string databaseName;
            if (cmd.TryGet(DatabaseName, out databaseName) == false)
                throw new ArgumentException("Update database command must contain a DatabaseName property");

            try
            {
                var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
                var dbKey = "db/" + databaseName;

                Slice valueName;
                Slice valueNameLowered;
                using (Slice.From(context.Allocator, dbKey, out valueName))
                using (Slice.From(context.Allocator, dbKey.ToLowerInvariant(), out valueNameLowered))
                {
                    long etag;
                    var doc = ReadInternal(context, out etag, valueNameLowered);
                    
                    if (doc == null)
                    {
                        NotifyLeaderAboutError(index, leader, new CommandExecutionException($"Cannot execute update command of type {type} for {databaseName} because it does not exists"));
                        return;
                    }
                    
                    var databaseRecord = JsonDeserializationCluster.DatabaseRecord(doc);
                    var updateCommand = JsonDeserializationCluster.UpdateDatabaseCommands[type](cmd);

                    if (updateCommand.Etag != null && etag != updateCommand.Etag.Value)
                    {
                        NotifyLeaderAboutError(index, leader,
                            new ConcurrencyException($"Concurrency violation at executing {type} command, the database {databaseRecord.DatabaseName} has etag {etag} but was expecting {updateCommand.Etag}"));
                        return;
                    }

                    try
                    {
                        updateCommand.UpdateDatabaseRecord(databaseRecord, index);
                    }
                    catch (Exception e)
                    {
                        NotifyLeaderAboutError(index, leader, new CommandExecutionException($"Cannot execute command of type {type} for database {databaseName}", e));
                        return;
                    }
                    
                    var updatedDatabaseBlittable = EntityToBlittable.ConvertEntityToBlittable(databaseRecord, DocumentConventions.Default, context);
                    UpdateDatabaseRecord(index, items, valueNameLowered, valueName, updatedDatabaseBlittable);
                    
                }
            }
            finally
            {
                NotifyDatabaseChanged(context, databaseName, index);

            }
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
            Slice loweredPrefix;
            using (Slice.From(context.Allocator, dbKey, out loweredPrefix))
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
            int size;
            var ptr = result.Reader.Read(2, out size);
            var doc = new BlittableJsonReaderObject(ptr, size, context);

            var key = Encoding.UTF8.GetString(result.Reader.Read(1, out size), size);

            return Tuple.Create(key, doc);
        }

        public DatabaseRecord ReadDatabase(TransactionOperationContext context, string name)
        {
            long etag;
            return ReadDatabase(context, name, out etag);
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
            long etag;
            return Read(context, name, out etag);
        }

        public BlittableJsonReaderObject Read(TransactionOperationContext context, string name, out long etag)
        {

            var dbKey = name.ToLowerInvariant();
            Slice key;
            using (Slice.From(context.Allocator, dbKey, out key))
            {
                return ReadInternal(context, out etag, key);
            }
        }

        private static unsafe BlittableJsonReaderObject ReadInternal(TransactionOperationContext context, out long etag, Slice key)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            TableValueReader reader;
            if (items.ReadByKey(key, out reader) == false)
            {
                etag = 0;
                return null;
            }

            int size;
            var ptr = reader.Read(2, out size);
            var doc = new BlittableJsonReaderObject(ptr, size, context);

            etag = Bits.SwapBytes(*(long*)reader.Read(3, out size));
            Debug.Assert(size == sizeof(long));
            
            return doc;
        }

        public static IEnumerable<(long, BlittableJsonReaderObject)> ReadValuesStartingWith(TransactionOperationContext context, string startsWithKey)
        {
            var startsWithKeyLower = startsWithKey.ToLowerInvariant();
            Slice startsWithSlice;
            using (Slice.From(context.Allocator, startsWithKeyLower, out startsWithSlice))
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
                int size;
                BlittableJsonReaderObject doc;
                long etag;
                size = GetDataAndEtagTupleFromReader(context, reader, out doc, out etag);
                Debug.Assert(size == sizeof(long));

                yield return (etag, doc);
            }
            
        }

        private static unsafe int GetDataAndEtagTupleFromReader(TransactionOperationContext context, TableValueReader reader, out BlittableJsonReaderObject doc, out long etag)
        {
            int size;
            var ptr = reader.Read(2, out size);
            doc = new BlittableJsonReaderObject(ptr, size, context);

            etag = Bits.SwapBytes(*(long*)reader.Read(3, out size));
            return size;
        }

        public override async Task<Stream> ConnectToPeer(string url, string apiKey)
        {
            var info = await ReplicationUtils.GetTcpInfoAsync(url, "Rachis.Server", apiKey, "Cluster");
            var authenticator = new ApiKeyAuthenticator();

            var tcpInfo = new Uri(info.Url);
            var tcpClient = new TcpClient();
            NetworkStream stream = null;
            try
            {
                await tcpClient.ConnectAsync(tcpInfo.Host, tcpInfo.Port);
                stream = tcpClient.GetStream();

                JsonOperationContext context;
                using (ContextPoolForReadOnlyOperations.AllocateOperationContext(out context))
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
                        onDatabaseChanged.Invoke(this, (db, lastIncludedIndex));
                }, null);
            }
        }
    }

    public class PutValueCommand
    {
        public string Name;
        public BlittableJsonReaderObject Value;
    }

    public class DeleteValueCommand
    {
        public string Name;
    }

    public class DeleteDatabaseCommand
    {
        public string DatabaseName;
        public bool HardDelete;
        public string FromNode;
    }

    public class AddDatabaseCommand
    {
        public string Name;
        public BlittableJsonReaderObject Value;
        public long? Etag;
        public bool Encrypted;
    }

    public class RemoveNodeFromDatabaseCommand
    {
        public string DatabaseName;
        public string NodeTag;
    }

    public class RachisLogIndexNotifications
    {
        private long _lastModifiedIndex;
        private readonly AsyncManualResetEvent _notifiedListeners;

        public RachisLogIndexNotifications(CancellationToken token)
        {
            _notifiedListeners = new AsyncManualResetEvent(token);
        }

        public async Task WaitForIndexNotification(long index)
        {
            while (index > Volatile.Read(ref _lastModifiedIndex))
                await _notifiedListeners.WaitAsync();
        }

        public void NotifyListenersAbout(long index)
        {
            var lastModifed = _lastModifiedIndex;
            while (index > lastModifed)
                lastModifed = Interlocked.CompareExchange(ref _lastModifiedIndex, index, lastModifed);
            _notifiedListeners.SetAndResetAtomically();
        }
    }
}