using System.Collections.Generic;
using Sparrow.Json;
using Voron;
using Voron.Data.Tables;
using static Raven.Server.ServerWide.ClusterStateMachine;
using static Raven.Server.Documents.DocumentsStorage;
using static Raven.Server.ServerWide.Commands.CompareExchangeCommandBase;

namespace Raven.Server.Storage.Schema.Updates.Server
{
    public unsafe class From11 : ISchemaUpdate
    {
        public bool Update(UpdateStep step)
        {
            var dbs = new List<string>();
            const string dbKey = "db/";

            var oldCompareExchangeSchema = new TableSchema().
                DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)UniqueItems.Key,
                Count = 1
            });

            var items = step.ReadTx.OpenTable(ItemsSchema, Items);
            using (Slice.From(step.ReadTx.Allocator, dbKey, out Slice loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
                {
                    dbs.Add(GetCurrentItemKey(result.Value).Substring(dbKey.Length));
                }
            }

            foreach (var db in dbs)
            {
                // update CompareExchange
                var readTable = step.ReadTx.OpenTable(oldCompareExchangeSchema, CompareExchange);
                if (readTable != null)
                {
                    var writeOldTable = step.WriteTx.OpenTable(oldCompareExchangeSchema, CompareExchange);
                    var writeTable = step.WriteTx.OpenTable(CompareExchangeSchema, CompareExchange);

                    using (Slice.From(step.ReadTx.Allocator, db.ToLowerInvariant() + "/", out var keyPrefix))
                    {
                        foreach (var item in readTable.SeekByPrimaryKeyPrefix(keyPrefix, Slices.Empty, 0))
                        {
                            var index = TableValueToLong((int)UniqueItems.Index, ref item.Value.Reader);
                            GetPrefixIndexSlices(step.ReadTx.Allocator, db, index, out var indexTuple);

                            using (indexTuple.Scope)
                            using (Slice.External(step.WriteTx.Allocator, indexTuple.Buffer.Ptr, indexTuple.Buffer.Length, out var prefixIndexSlice))
                            using (writeTable.Allocate(out TableValueBuilder write))
                            using (var ctx = JsonOperationContext.ShortTermSingleUse())
                            {
                                var bjro = new BlittableJsonReaderObject(item.Value.Reader.Read((int)UniqueItems.Value, out var size1), size1, ctx);
                                write.Add(item.Key);
                                write.Add(index);
                                write.Add(bjro.BasePointer, bjro.Size);
                                write.Add(prefixIndexSlice);

                                writeOldTable.DeleteByKey(item.Key);
                                writeTable.Insert(write);
                            }
                        }
                    }
                }
                var identities = step.ReadTx.ReadTree(Identities);

                if (identities != null)
                {
                    step.WriteTx.DeleteTree(ServerWide.ClusterStateMachine.Identities);
                    Slice.From(step.WriteTx.Allocator, "Identities", out var Identities);
                    IdentitiesSchema.Create(step.WriteTx, Identities, 32);
                    var writeTable = step.WriteTx.OpenTable(IdentitiesSchema, Identities);
                    using (Slice.From(step.ReadTx.Allocator, $"{dbKey}{db.ToLowerInvariant()}/identities/", out var identityPrefix))
                    {
                        using (var it = identities.Iterate(prefetch: false))
                        {
                            if (it.Seek(identityPrefix) == false)
                                continue;

                            do
                            {
                                var key = it.CurrentKey;
                                var keyAsString = key.ToString();   // old identity key
                                var value = it.CreateReaderForCurrent().ReadLittleEndianInt64();

                                var newKey = keyAsString.Substring(identityPrefix.ToString().Length);
                                var index = 0;

                                // write to new identities schema
                                GetKeyAndPrefixIndexSlices(step.ReadTx.Allocator, db, $"{newKey}", index, out var keyTuple, out var indexTuple);
                                using (keyTuple.Scope)
                                using (indexTuple.Scope)
                                using (Slice.External(step.ReadTx.Allocator, keyTuple.Buffer.Ptr, keyTuple.Buffer.Length, out var keySlice))
                                using (Slice.External(step.ReadTx.Allocator, indexTuple.Buffer.Ptr, indexTuple.Buffer.Length, out var prefixIndexSlice))
                                {
                                    using (writeTable.Allocate(out var write))
                                    {
                                        write.Add(keySlice);
                                        write.Add(value);
                                        write.Add(index);
                                        write.Add(prefixIndexSlice);

                                        writeTable.Set(write);
                                    }
                                }
                            } while (it.MoveNext());
                        }
                    }
                }
            }

            return true;
        }
    }
}
