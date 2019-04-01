using System.Collections.Generic;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Voron;
using Voron.Data.Tables;
using static Raven.Server.Documents.DocumentsStorage;
using static Raven.Server.ServerWide.Commands.CompareExchangeCommandBase;

namespace Raven.Server.Storage.Schema.Updates.Server
{
    public unsafe class From13 : ISchemaUpdate
    {
        public bool Update(UpdateStep step)
        {
            var dbs = new List<string>();
            const string dbKey = "db/";

            var oldCompareExchangeSchema = new TableSchema().
                DefineKey(new TableSchema.SchemaIndexDef
                {
                    StartIndex = (int)ClusterStateMachine.CompareExchangeTable.Key,
                    Count = 1
                });

            var newCompareExchangeSchema = new TableSchema()
                .DefineKey(new TableSchema.SchemaIndexDef
                {
                    StartIndex = (int)ClusterStateMachine.CompareExchangeTable.Key,
                    Count = 1
                }).DefineIndex(new TableSchema.SchemaIndexDef
                {
                    StartIndex = (int)ClusterStateMachine.CompareExchangeTable.PrefixIndex,
                    Count = 1,
                    IsGlobal = true,
                    Name = ClusterStateMachine.CompareExchangeIndex,
                    Dangerous_IgnoreForDeletes = true
                });

            var oldIdentitiesSchema = new TableSchema().
                DefineKey(new TableSchema.SchemaIndexDef
                {
                    StartIndex = (int)ClusterStateMachine.IdentitiesTable.Key,
                    Count = 1
                });

            var newIdentitiesSchema = new TableSchema()
                .DefineKey(new TableSchema.SchemaIndexDef
                {
                    StartIndex = (int)ClusterStateMachine.IdentitiesTable.Key,
                    Count = 1
                }).DefineIndex(new TableSchema.SchemaIndexDef
                {
                    StartIndex = (int)ClusterStateMachine.IdentitiesTable.KeyIndex,
                    Count = 1,
                    IsGlobal = true,
                    Name = ClusterStateMachine.IdentitiesIndex,
                    Dangerous_IgnoreForDeletes = true
                });

            using (var items = step.ReadTx.OpenTable(ClusterStateMachine.ItemsSchema, ClusterStateMachine.Items))
            using (Slice.From(step.ReadTx.Allocator, dbKey, out Slice loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
                {
                    dbs.Add(ClusterStateMachine.GetCurrentItemKey(result.Value).Substring(dbKey.Length));
                }
            }

            foreach (var db in dbs)
            {
                var dbPrefixLowered = $"{db.ToLowerInvariant()}/";

                // update CompareExchangeSchema
                var readCompareExchangeTable = step.ReadTx.OpenTable(oldCompareExchangeSchema, ClusterStateMachine.CompareExchange);

                if (readCompareExchangeTable != null)
                {
                    using (Slice.From(step.ReadTx.Allocator, dbPrefixLowered, out var keyPrefix))
                    {
                        var writeCompareExchangeTable = step.WriteTx.OpenTable(newCompareExchangeSchema, ClusterStateMachine.CompareExchange);
                        writeCompareExchangeTable.Danger_NoInPlaceUpdates = true;
                        foreach (var item in readCompareExchangeTable.SeekByPrimaryKeyPrefix(keyPrefix, Slices.Empty, 0))
                        {
                            var index = TableValueToLong((int)ClusterStateMachine.CompareExchangeTable.Index, ref item.Value.Reader);

                            using (GetPrefixIndexSlices(step.ReadTx.Allocator, db, index, out var buffer))
                            using (Slice.External(step.WriteTx.Allocator, buffer.Ptr, buffer.Length, out var prefixIndexSlice))
                            using (writeCompareExchangeTable.Allocate(out TableValueBuilder write))
                            using (var ctx = JsonOperationContext.ShortTermSingleUse())
                            {
                                using (var bjro = new BlittableJsonReaderObject(item.Value.Reader.Read((int)ClusterStateMachine.CompareExchangeTable.Value, out var size1), size1, ctx).Clone(ctx))
                                {
                                    write.Add(item.Key);
                                    write.Add(index);
                                    write.Add(bjro.BasePointer, bjro.Size);
                                    write.Add(prefixIndexSlice);

                                    writeCompareExchangeTable.Set(write);
                                }
                            }
                        }
                    }
                }

                // update IdentitiesSchema
                var readIdentitiesTable = step.ReadTx.OpenTable(oldIdentitiesSchema, ClusterStateMachine.Identities);
                if (readIdentitiesTable != null)
                {
                    using (Slice.From(step.ReadTx.Allocator, dbPrefixLowered, out var keyPrefix))
                    {
                        var writeIdentitiesTable = step.WriteTx.OpenTable(newIdentitiesSchema, ClusterStateMachine.Identities);
                        writeIdentitiesTable.Danger_NoInPlaceUpdates = true;
                        foreach (var item in readIdentitiesTable.SeekByPrimaryKeyPrefix(keyPrefix, Slices.Empty, 0))
                        {
                            var index = TableValueToLong((int)ClusterStateMachine.IdentitiesTable.Index, ref item.Value.Reader);
                            var value = TableValueToLong((int)ClusterStateMachine.IdentitiesTable.Value, ref item.Value.Reader);

                            using (GetPrefixIndexSlices(step.ReadTx.Allocator, db, index, out var buffer))
                            using (Slice.External(step.WriteTx.Allocator, buffer.Ptr, buffer.Length, out var prefixIndexSlice))
                            using (writeIdentitiesTable.Allocate(out TableValueBuilder write))
                            {
                                write.Add(item.Key);
                                write.Add(value);
                                write.Add(index);
                                write.Add(prefixIndexSlice);

                                writeIdentitiesTable.Set(write);
                            }
                        }
                    }
                }
            }

            return true;
        }
    }
}
