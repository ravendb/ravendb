using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Sparrow.Json;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Storage.Schema.Updates.Server
{
    public unsafe class From42017 : ISchemaUpdate
    {
        public int From => 42_017;

        public int To => 42_018;

        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Server;

        public bool Update(UpdateStep step)
        {
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
                    Name = ClusterStateMachine.CompareExchangeIndex
                });

            const string oldTableName = "CmpXchg";
            using (Slice.From(step.WriteTx.Allocator, oldTableName, out var oldCompareExchangeTable))
            {
                var oldTable = step.ReadTx.OpenTable(oldCompareExchangeSchema, oldCompareExchangeTable);
                if (oldTable == null)
                    return true;

                var newTableName = ClusterStateMachine.CompareExchange.ToString();
                foreach (var db in SchemaUpgradeExtensions.GetDatabases(step))
                {
                    // update CompareExchange
                    newCompareExchangeSchema.Create(step.WriteTx, newTableName, null);
                    var newTable = step.WriteTx.OpenTable(newCompareExchangeSchema, newTableName);
                    var compareExchangeOldKey = $"{db.ToLowerInvariant()}/";

                    using (Slice.From(step.ReadTx.Allocator, compareExchangeOldKey, out var keyPrefix))
                    {
                        foreach (var item in oldTable.SeekByPrimaryKeyPrefix(keyPrefix, Slices.Empty, 0))
                        {
                            var index = DocumentsStorage.TableValueToLong((int)ClusterStateMachine.CompareExchangeTable.Index, ref item.Value.Reader);

                            using (CompareExchangeCommandBase.GetPrefixIndexSlices(step.ReadTx.Allocator, db, index, out var buffer))
                            using (Slice.External(step.WriteTx.Allocator, buffer.Ptr, buffer.Length, out var prefixIndexSlice))
                            using (newTable.Allocate(out TableValueBuilder write))
                            using (var ctx = JsonOperationContext.ShortTermSingleUse())
                            {
                                using (var bjro = new BlittableJsonReaderObject(
                                        item.Value.Reader.Read((int)ClusterStateMachine.CompareExchangeTable.Value, out var size1),
                                        size1, ctx).Clone(ctx)
                                )
                                {
                                    write.Add(item.Key);
                                    write.Add(index);
                                    write.Add(bjro.BasePointer, bjro.Size);
                                    write.Add(prefixIndexSlice);

                                    newTable.Set(write);
                                }
                            }
                        }
                    }
                }
            }

            // delete the old table
            step.WriteTx.DeleteTable(oldTableName);

            // remove the remaining CompareExchange global index
            if (step.WriteTx.LowLevelTransaction.RootObjects.Read(ClusterStateMachine.CompareExchangeIndex) != null)
                step.WriteTx.DeleteTree(ClusterStateMachine.CompareExchangeIndex);

            return true;
        }
    }
}
