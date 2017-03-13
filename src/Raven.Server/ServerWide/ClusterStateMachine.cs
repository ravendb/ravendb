using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Transformers;
using Raven.Server.Config.Settings;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
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
                case nameof(DeleteValueCommand):
                    DeleteValue(context, cmd, index, leader);
                    break;
                case nameof(PutValueCommand):
                    PutValue(context, cmd, index, leader);
                    break;

                case nameof(TEMP_DelDatabaseCommand):
                    TEMP_DeleteValue(context, cmd, index, leader);
                    break;
                case nameof(TEMP_SetDatabaseCommand):
                    TEMP_PutValue(context, cmd, index, leader);
                    break;
            }
        }

        private static void TEMP_DeleteValue(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);
            var delCmd = JsonDeserializationCluster.DeleteValueCommand(cmd);
            Slice str;
            using (Slice.From(context.Allocator, delCmd.Name, out str))
            {
                items.DeleteByKey(str);
            }
        }

        private static unsafe void TEMP_PutValue(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
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

        private  unsafe void AddDatabase(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
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

                    context.Transaction.InnerTransaction.LowLevelTransaction.OnCommit += transaction =>
                    {
                        Task.Run(() =>
                        {
                            DatabaseChanged?.Invoke(this, databaseName);
                        });
                    };
                }
            }
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
                foreach (var result in items.SeekByPrimaryKeyStartingWith(loweredPrefix, Slices.Empty, start))
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

        public BlittableJsonReaderObject Read(TransactionOperationContext context, string name)
        {
            long etag;
            return Read(context, name, out etag);
        }

        public unsafe BlittableJsonReaderObject Read(TransactionOperationContext context, string name, out long etag)
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

            var dbKey = name.ToLowerInvariant();
            Slice key;
            using (Slice.From(context.Allocator, dbKey, out key))
            {
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

    public class AddDatabaseCommand
    {
        public string DatabaseName;

        public string DataDirectory;

        public DatabaseTopology Topology;
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

    public class DatabaseTopology
    {
        public string[] Members;
        public string[] Promotables;
        public string[] Watchers;
    }

    public class DatabaseRecord
    {
        public string DatabaseName;

        public bool Disabled;

        public bool DeletionInProgress;

        public string DataDirectory;

        public DatabaseTopology Topology;

        public IndexDefinition[] Indexes;

        public TransformerDefinition[] Transformers;
    }

    public class JsonDeserializationCluster : JsonDeserializationBase
    {
        public static readonly Func<BlittableJsonReaderObject, TEMP_SetDatabaseCommand> TEMP_SetDatabaseCommand = GenerateJsonDeserializationRoutine<TEMP_SetDatabaseCommand>();

        public static readonly Func<BlittableJsonReaderObject, PutValueCommand> PutValueCommand = GenerateJsonDeserializationRoutine<PutValueCommand>();

        public static readonly Func<BlittableJsonReaderObject, DeleteValueCommand> DeleteValueCommand = GenerateJsonDeserializationRoutine<DeleteValueCommand>();

        public static readonly Func<BlittableJsonReaderObject, AddDatabaseCommand> AddDatabaseCommand = GenerateJsonDeserializationRoutine<AddDatabaseCommand>();
        public static readonly Func<BlittableJsonReaderObject, DatabaseRecord> DatabaseRecord = GenerateJsonDeserializationRoutine<DatabaseRecord>();
    }
}