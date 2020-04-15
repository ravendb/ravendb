using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;
using static Raven.Server.Documents.DocumentsStorage;
using static Raven.Server.ServerWide.Commands.CompareExchangeCommandBase;

namespace Raven.Server.Storage.Schema.Updates.Server
{
    public unsafe class From42014 : ISchemaUpdate
    {
        public int From => 42_014;

        public int To => 42_015;

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
                            var value = TableValueToLong((int)ClusterStateMachine.IdentitiesTable.Value, ref item.Value.Reader);
                            var index = TableValueToLong((int)ClusterStateMachine.IdentitiesTable.Index, ref item.Value.Reader);

                            // if value is not 0 than we come from v4.2 and could have wrong index value
                            if (index != 0)
                            {
                                using (GetPrefixIndexSlices(step.ReadTx.Allocator, db, 0L, out var buffer))
                                using (Slice.External(step.WriteTx.Allocator, buffer.Ptr, buffer.Length, out var prefixIndexSlice))
                                using (writeIdentitiesTable.Allocate(out TableValueBuilder write))
                                {
                                    write.Add(item.Key);
                                    write.Add(value);
                                    write.Add(0L);
                                    write.Add(prefixIndexSlice);

                                    writeIdentitiesTable.Set(write);
                                }
                            }
                        }
                    }
                }

                // update db backup status
                var dbLower = db.ToLowerInvariant();
                using (var items = step.WriteTx.OpenTable(ClusterStateMachine.ItemsSchema, ClusterStateMachine.Items))
                using (Slice.From(step.ReadTx.Allocator, $"{dbKey}{dbLower}", out Slice lowerKey))
                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                {
                    var (databaseRecordJson, _) = GetBjroAndIndex(ctx, items, lowerKey);
                    var databaseRecord = JsonDeserializationCluster.DatabaseRecord(databaseRecordJson);
                    if (databaseRecord == null)
                        continue;

                    foreach (var pb in databaseRecord.PeriodicBackups)
                    {
                        var pbItemName = PeriodicBackupStatus.GenerateItemName(db, pb.TaskId);
                        using (Slice.From(step.WriteTx.Allocator, pbItemName, out Slice pbsSlice))
                        using (Slice.From(step.WriteTx.Allocator, pbItemName.ToLowerInvariant(), out Slice pbsSliceLower))
                        {
                            var (singleBackupStatus, index) = GetBjroAndIndex(ctx, items, pbsSlice);
                            if (singleBackupStatus == null)
                                continue;

                            if (singleBackupStatus.TryGet(nameof(PeriodicBackupStatus.LocalBackup), out BlittableJsonReaderObject localBackup) == false
                                || singleBackupStatus.TryGet(nameof(PeriodicBackupStatus.LastRaftIndex), out BlittableJsonReaderObject lastRaftIndexBlittable) == false)
                                continue;

                            if (localBackup.TryGet(nameof(PeriodicBackupStatus.LastIncrementalBackup), out DateTime? lastIncrementalBackupDate) == false
                                || lastRaftIndexBlittable.TryGet(nameof(PeriodicBackupStatus.LastEtag), out long? lastRaftIndex) == false)
                                continue;

                            if (lastIncrementalBackupDate == null || lastRaftIndex == null)
                                continue;

                            // already set in from12 before
                            if (lastRaftIndex == 0)
                                continue;

                            var myLastRaftIndex = new LastRaftIndex
                            {
                                LastEtag = 0L
                            };

                            singleBackupStatus.Modifications = new DynamicJsonValue
                            {
                                [nameof(PeriodicBackupStatus.LastRaftIndex)] = myLastRaftIndex.ToJson()
                            };

                            using (var old = singleBackupStatus)
                            {
                                singleBackupStatus = ctx.ReadObject(singleBackupStatus, pbItemName);
                            }

                            using (items.Allocate(out var builder))
                            {
                                builder.Add(pbsSliceLower);
                                builder.Add(pbsSlice);
                                builder.Add(singleBackupStatus.BasePointer, singleBackupStatus.Size);
                                builder.Add(index);

                                items.Set(builder);
                            }
                        }
                    }

                }
            }

            return true;
        }

        private (BlittableJsonReaderObject, long) GetBjroAndIndex(JsonOperationContext context, Table items, Slice lowerKeySlice)
        {
            if (items.ReadByKey(lowerKeySlice, out TableValueReader reader) == false)
                return (null, 0L);

            // We use the follow format for the items data
            // { lowered key, key, data, etag }
            const int data = 2;
            const int etag = 3;

            var ptr = reader.Read(data, out int size);
            var bjro = new BlittableJsonReaderObject(ptr, size, context);

            var index = *(long*)reader.Read(etag, out var sizeOfLong);
            Debug.Assert(sizeOfLong == sizeof(long));

            return (bjro, index);
        }
    }
}
