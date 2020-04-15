using System.Collections.Generic;
using Raven.Server.ServerWide;
using Voron;
using Voron.Data.Tables;
using static Raven.Server.Documents.DocumentsStorage;
using static Raven.Server.ServerWide.Commands.CompareExchangeCommandBase;

namespace Raven.Server.Storage.Schema.Updates.Server
{
    public unsafe class From42013 : ISchemaUpdate
    {
        public int From => 42_013;

        public int To => 42_014;

        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Server;

        public bool Update(UpdateStep step)
        {
            var dbs = new List<string>();
            const string dbKey = "db/";

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
                    Name = ClusterStateMachine.IdentitiesIndex
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

                // update IdentitiesSchema
                var readIdentitiesTable = step.ReadTx.OpenTable(ClusterStateMachine.IdentitiesSchema, ClusterStateMachine.Identities);
                if (readIdentitiesTable != null)
                {
                    using (Slice.From(step.ReadTx.Allocator, dbPrefixLowered, out var keyPrefix))
                    {
                        var writeIdentitiesTable = step.WriteTx.OpenTable(newIdentitiesSchema, ClusterStateMachine.Identities);
                        foreach (var item in readIdentitiesTable.SeekByPrimaryKeyPrefix(keyPrefix, Slices.Empty, 0))
                        {
                            var indexPtr = item.Value.Reader.Read((int)ClusterStateMachine.IdentitiesTable.Index, out var size);

                            // we come from v4.2 RC1
                            if (size == sizeof(int))
                            {
                                var indexIntValue = *(int*)indexPtr;
                                long index = (long)indexIntValue;
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
            }

            return true;
        }
    }
}
