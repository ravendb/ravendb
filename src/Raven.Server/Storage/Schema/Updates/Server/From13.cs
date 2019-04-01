using System;
using System.Collections.Generic;
using System.Diagnostics;
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
                var writeCompareExchangeTable = step.WriteTx.OpenTable(ClusterStateMachine.CompareExchangeSchema, ClusterStateMachine.CompareExchange);
                if (writeCompareExchangeTable != null)
                {
                    using (Slice.From(step.ReadTx.Allocator, dbPrefixLowered, out var keyPrefix))
                    {
                        foreach (var item in writeCompareExchangeTable.SeekByPrimaryKeyPrefix(keyPrefix, Slices.Empty, 0))
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

                                    writeCompareExchangeTable.DeleteByKey(item.Key);
                                    writeCompareExchangeTable.Set(write);
                                }
                            }
                        }
                    }
                }

                // update IdentitiesSchema
                var writeIdentitiesTable = step.WriteTx.OpenTable(ClusterStateMachine.IdentitiesSchema, ClusterStateMachine.Identities);
                if (writeIdentitiesTable != null)
                {
                    using (Slice.From(step.ReadTx.Allocator, dbPrefixLowered, out var keyPrefix))
                    {
                        foreach (var item in writeIdentitiesTable.SeekByPrimaryKeyPrefix(keyPrefix, Slices.Empty, 0))
                        {
                            var index = TableValueToLong((int)ClusterStateMachine.IdentitiesTable.Index, ref item.Value.Reader);
                            var value = TableValueToLong((int)ClusterStateMachine.IdentitiesTable.Value, ref item.Value.Reader);

                            using (GetPrefixIndexSlices(step.ReadTx.Allocator, db, index, out var buffer))
                            using (Slice.External(step.WriteTx.Allocator, buffer.Ptr, buffer.Length, out var prefixIndexSlice))
                            using (writeIdentitiesTable.Allocate(out TableValueBuilder write))
                            {
                                    write.Add(item.Key);
                                    write.Add(index);
                                    write.Add(value);
                                    write.Add(prefixIndexSlice);

                                    writeIdentitiesTable.DeleteByKey(item.Key);
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
