using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Transformers;
using Raven.Client.Exceptions.Security;
using Raven.Client.Http.OAuth;
using Raven.Client.Server.Commands;
using Raven.Client.Server.Tcp;
using Raven.Server.Documents.Versioning;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data;
using Voron.Data.Tables;

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

        private readonly AsyncManualResetEvent _notifiedListeners = new AsyncManualResetEvent();
        private long _lastNotified;

        public async Task WaitForIndexNotification(long index)
        {
            var task = _notifiedListeners.WaitAsync();

            while (index > Volatile.Read(ref _lastNotified))
            {
                await task;

                task = _notifiedListeners.WaitAsync();
            }
        }

        public event EventHandler<string> DatabaseChanged;

        protected override void Apply(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            string type;
            if (cmd.TryGet("Type", out type) == false)
                return;


            switch (type)
            {
                case nameof(AddDatabaseCommand):
                    AddDatabase(context, cmd, index, leader);
                    break;
                case nameof(DeleteDatabaseCommand):
                    DeleteDatabase(context, cmd, index, leader);
                    break;

                case nameof(RemoveNodeFromDatabaseCommand):
                    RemoveNodeFromDatabase(context, cmd, index, leader);
                    break;

                case nameof(DeleteValueCommand):
                    DeleteValue(context, cmd, index, leader);
                    break;
                case nameof(EditVersioningCommand):
                    EditVersioning(context, cmd, index, leader);
                    break;
                case nameof(PutValueCommand):
                    PutValue(context, cmd, index, leader);
                    break;

                case nameof(TEMP_DelDatabaseCommand):
                    TEMP_DeleteValue(context, cmd, index, leader);
                    break;
                case nameof(TEMP_SetDatabaseCommand):
                    TEMP_SetDatabaseValue(context, cmd, index, leader);
                    break;
            }
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
                    NotifyLeaderAboutError(index, leader, $"The database {databaseName} does not exists");
                    return;
                }
                int size;
                var doc = new BlittableJsonReaderObject(reader.Read(2, out size), size, context);

                var databaseRecord = JsonDeserializationCluster.DatabaseRecord(doc);

                BlittableJsonReaderObject topology;
                if (doc.TryGet(nameof(DatabaseRecord.Topology), out topology) == false)
                {
                    items.DeleteByKey(loweredKey);
                    return;
                }

                databaseRecord.Topology.Members.Remove(remove.NodeTag);
                databaseRecord.Topology.Promotables.Remove(remove.NodeTag);
                databaseRecord.Topology.Watchers.Remove(remove.NodeTag);

                if (databaseRecord.Topology.Members.Count == 0 &&
                    databaseRecord.Topology.Promotables.Count == 0 &&
                    databaseRecord.Topology.Watchers.Count == 0)
                {
                    items.DeleteByKey(loweredKey);
                    return;
                }

                var entityToBlittable = new EntityToBlittable(null);
                var updated = entityToBlittable.ConvertEntityToBlittable(databaseRecord, DocumentConventions.Default, context);

                TableValueBuilder builder;
                using (items.Allocate(out builder))
                {
                    builder.Add(loweredKey);
                    builder.Add(key);
                    builder.Add(updated.BasePointer, updated.Size);
                    builder.Add(index);

                    items.Set(builder);
                }

                context.Transaction.InnerTransaction.LowLevelTransaction.OnCommit += transaction =>
                {
                    Task.Run(() =>
                    {
                        DatabaseChanged?.Invoke(this, databaseName);
                    });
                };
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
                    NotifyLeaderAboutError(index, leader, $"The database {databaseName} does not exists");
                    return;
                }

                int size;
                var doc = new BlittableJsonReaderObject(reader.Read(2, out size), size, context);

                doc.Modifications = new DynamicJsonValue(doc)
                {
                    [nameof(DatabaseRecord.DeletionInProgress)] =
                        delDb.HardDelete
                            ? DeletionInProgressStatus.HardDelete
                            : DeletionInProgressStatus.SoftDelete
                };

                using (var updated = context.ReadObject(doc, databaseName))
                {
                    TableValueBuilder builder;
                    using (items.Allocate(out builder))
                    {
                        builder.Add(loweredKey);
                        builder.Add(key);
                        builder.Add(updated.BasePointer, updated.Size);
                        builder.Add(index);

                        items.Set(builder);
                    }
                }

                context.Transaction.InnerTransaction.LowLevelTransaction.OnCommit += transaction =>
                {
                    Task.Run(() =>
                    {
                        DatabaseChanged?.Invoke(this, databaseName);
                    });
                };
            }
        }

        private void TEMP_DeleteValue(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            var delCmd = JsonDeserializationCluster.DeleteValueCommand(cmd);
            Slice str;
            using (Slice.From(context.Allocator, delCmd.Name, out str))
            {
                items.DeleteByKey(str);
            }


            context.Transaction.InnerTransaction.LowLevelTransaction.OnCommit += transaction =>
            {
                Task.Run(() =>
                {
                    DatabaseChanged?.Invoke(this, delCmd.Name.Replace("db/", ""));
                });
            };
        }

        private unsafe void TEMP_SetDatabaseValue(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            var setDb = JsonDeserializationCluster.TEMP_SetDatabaseCommand(cmd);

            TableValueBuilder builder;
            Slice valueName, valueNameLowered;
            using (items.Allocate(out builder))
            using (Slice.From(context.Allocator, setDb.Name, out valueName))
            using (Slice.From(context.Allocator, setDb.Name.ToLowerInvariant(), out valueNameLowered))
            using (var rec = context.ReadObject(setDb.Value, "inner-val"))
            {
                if (setDb.Etag != null)
                {
                    TableValueReader reader;
                    if (items.ReadByKey(valueNameLowered, out reader) == false && setDb.Etag != 0)
                    {
                        NotifyLeaderAboutError(index, leader, "Concurrency violation, the database " + setDb.Name + " does not exists, but had a non zero etag");
                        return;
                    }

                    int size;
                    var actualEtag = *(long*)reader.Read(3, out size);
                    Debug.Assert(size == sizeof(long));

                    if (actualEtag != setDb.Etag.Value)
                    {
                        NotifyLeaderAboutError(index, leader,
                            "Concurrency violation, the database " + setDb.Name + " has etag " + actualEtag + " but was expecting " + setDb.Etag);
                        return;
                    }
                }

                builder.Add(valueNameLowered);
                builder.Add(valueName);
                builder.Add(rec.BasePointer, rec.Size);
                builder.Add(index);

                items.Set(builder);

                context.Transaction.InnerTransaction.LowLevelTransaction.OnCommit += transaction =>
                {
                    Task.Run(() =>
                    {
                        DatabaseChanged?.Invoke(this, setDb.Name.Replace("db/", ""));
                    });
                };
            }
        }

        private static void DeleteValue(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            var delCmd = JsonDeserializationCluster.DeleteValueCommand(cmd);
            if (delCmd.Name.StartsWith("db/"))
            {
                NotifyLeaderAboutError(index, leader, "Cannot set " + delCmd.Name + " using DeleteValueCommand, only via dedicated Database calls");
                return;
            }
            Slice str;
            using (Slice.From(context.Allocator, delCmd.Name, out str))
            {
                items.DeleteByKey(str);
            }
        }

        private static unsafe void PutValue(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            var putVal = JsonDeserializationCluster.PutValueCommand(cmd);
            if (putVal.Name.StartsWith("db/"))
            {
                NotifyLeaderAboutError(index, leader, "Cannot set " + putVal.Name + " using PutValueCommand, only via dedicated Database calls");
                return;
            }

            TableValueBuilder builder;
            Slice valueName, valueNameLowered;
            using (items.Allocate(out builder))
            using (Slice.From(context.Allocator, putVal.Name, out valueName))
            using (Slice.From(context.Allocator, putVal.Name.ToLowerInvariant(), out valueNameLowered))
            using (var rec = context.ReadObject(putVal.Value, "inner-val"))
            {
                builder.Add(valueNameLowered);
                builder.Add(valueName);
                builder.Add(rec.BasePointer, rec.Size);
                builder.Add(index);

                items.Set(builder);
            }
        }

        private unsafe void AddDatabase(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            var addDb = JsonDeserializationCluster.AddDatabaseCommand(cmd);

            var databaseName = addDb.DatabaseName;
            var dbKey = "db/" + databaseName.ToLowerInvariant();
            Slice key;
            using (Slice.From(context.Allocator, dbKey, out key))
            {
                TableValueReader reader;
                if (items.ReadByKey(key, out reader))
                {
                    NotifyLeaderAboutError(index, leader, $"Cannot create database {databaseName} because it already exists");
                    return;
                }

                TableValueBuilder builder;
                Slice databaseNameSlice;
                using (items.Allocate(out builder))
                using (Slice.From(context.Allocator, databaseName, out databaseNameSlice))
                using (var dbRec = context.ReadObject(new DynamicJsonValue
                {
                    [nameof(DatabaseRecord.DatabaseName)] = databaseNameSlice,
                    [nameof(DatabaseRecord.DataDirectory)] = addDb.DataDirectory,
                    [nameof(DatabaseRecord.Topology)] = new DynamicJsonValue
                    {
                        [nameof(DatabaseTopology.Members)] = new DynamicJsonArray(addDb.Topology.Members),
                        [nameof(DatabaseTopology.Promotables)] = new DynamicJsonArray(addDb.Topology.Promotables),
                        [nameof(DatabaseTopology.Watchers)] = new DynamicJsonArray(addDb.Topology.Watchers),
                    },
                }, databaseName))
                {
                    builder.Add(key);
                    builder.Add(databaseNameSlice);
                    builder.Add(dbRec.BasePointer, dbRec.Size);
                    builder.Add(index);

                    items.Set(builder);

                    NotifyDatabaseChanged(context, databaseName, index);
                }
            }
        }

        private void NotifyDatabaseChanged(TransactionOperationContext context, string databaseName, long index)
        {
            context.Transaction.InnerTransaction.LowLevelTransaction.OnCommit += transaction =>
            {
                Task.Run(() =>
                {
                    try
                    {
                        DatabaseChanged?.Invoke(this, databaseName);
                    }
                    finally
                    {
                        var lastNotified = _lastNotified;
                        while (lastNotified < index)
                        {
                            var result = Interlocked.CompareExchange(ref _lastNotified, index, lastNotified);
                            if (result == lastNotified)
                                break;
                            lastNotified = result;
                        }
                        _notifiedListeners.Set();
                    }
                });
            };
        }

        private unsafe void EditVersioning(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            var editVersioning = JsonDeserializationCluster.EditVersioningCommand(cmd);
            var databaseName = editVersioning.Name;
            var dbKey = "db/" + databaseName;

            long etag;
            Slice valueName;
            Slice valueNameLowered;
            using (Slice.From(context.Allocator, dbKey, out valueName))
            using (Slice.From(context.Allocator, dbKey.ToLowerInvariant(), out valueNameLowered))
            {
                var doc = ReadInternal(context, out etag, valueNameLowered);
                if (doc == null)
                {
                    NotifyLeaderAboutError(index, leader, $"Cannot edit versioning on {databaseName} because it does not exists");
                    return;
                }
                var record = JsonDeserializationCluster.DatabaseRecord(doc);

                record.VersioningConfiguration = editVersioning.Configuration;

                var entityToBlittable = new EntityToBlittable(null);
                var updated = entityToBlittable.ConvertEntityToBlittable(record, DocumentConventions.Default, context);


                TableValueBuilder builder;
                using (items.Allocate(out builder))
                {
                    builder.Add(valueNameLowered);
                    builder.Add(valueName);
                    builder.Add(updated.BasePointer, updated.Size);
                    builder.Add(index);
                    items.Set(builder);
                }
            }
            NotifyDatabaseChanged(context, databaseName, index);
        }

        private static void NotifyLeaderAboutError(long index, Leader leader, string msg)
        {
            // ReSharper disable once UseNullPropagation
            if (leader == null)
                return;

            leader.SetStateOf(index, tcs =>
            {
                tcs.TrySetException(new InvalidOperationException(msg));
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

            etag = *(long*)reader.Read(3, out size);
            Debug.Assert(size == sizeof(long));

            return doc;
        }

        public override async Task<Stream> ConenctToPeer(string url, string apiKey)
        {
            var info = await ReplicationUtils.GetTcpInfoAsync(url, "Rachis.Server", apiKey);
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

    public class EditVersioningCommand
    {
        public string Name;
        public VersioningConfiguration Configuration;
    }

    public class AddDatabaseCommand
    {
        public string DatabaseName;

        public string DataDirectory;

        public DatabaseTopology Topology;

        public VersioningConfiguration VersioningConfiguration;
    }

    public class DeleteDatabaseCommand
    {
        public string DatabaseName;
        public bool HardDelete;
    }

    public class TEMP_SetDatabaseCommand
    {
        public string Name;
        public BlittableJsonReaderObject Value;
        public long? Etag;
    }

    public class TEMP_DelDatabaseCommand
    {
        public string Name;
        public BlittableJsonReaderObject Value;
    }

    public class RemoveNodeFromDatabaseCommand
    {
        public string DatabaseName;
        public string NodeTag;
    }

    public class DatabaseRecord
    {
        public string DatabaseName;

        public bool Disabled;

        public DeletionInProgressStatus DeletionInProgress;

        public string DataDirectory;

        public DatabaseTopology Topology;

        public IndexDefinition[] Indexes;

        public TransformerDefinition[] Transformers;

        public Dictionary<string, string> Settings;

        public VersioningConfiguration VersioningConfiguration;
    }

    public enum DeletionInProgressStatus
    {
        No,
        SoftDelete,
        HardDelete
    }

    public class JsonDeserializationCluster : JsonDeserializationBase
    {
        public static readonly Func<BlittableJsonReaderObject, TEMP_SetDatabaseCommand> TEMP_SetDatabaseCommand = GenerateJsonDeserializationRoutine<TEMP_SetDatabaseCommand>();

        public static readonly Func<BlittableJsonReaderObject, PutValueCommand> PutValueCommand = GenerateJsonDeserializationRoutine<PutValueCommand>();

        public static readonly Func<BlittableJsonReaderObject, DeleteValueCommand> DeleteValueCommand = GenerateJsonDeserializationRoutine<DeleteValueCommand>();

        public static readonly Func<BlittableJsonReaderObject, DeleteDatabaseCommand> DeleteDatabaseCommand = GenerateJsonDeserializationRoutine<DeleteDatabaseCommand>();

        public static readonly Func<BlittableJsonReaderObject, AddDatabaseCommand> AddDatabaseCommand = GenerateJsonDeserializationRoutine<AddDatabaseCommand>();
        public static readonly Func<BlittableJsonReaderObject, DatabaseRecord> DatabaseRecord = GenerateJsonDeserializationRoutine<DatabaseRecord>();
        public static readonly Func<BlittableJsonReaderObject, RemoveNodeFromDatabaseCommand> RemoveNodeFromDatabaseCommand = GenerateJsonDeserializationRoutine<RemoveNodeFromDatabaseCommand>();

        public static readonly Func<BlittableJsonReaderObject, EditVersioningCommand> EditVersioningCommand = GenerateJsonDeserializationRoutine<EditVersioningCommand>();
    }
}