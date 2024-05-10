using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;
using static Raven.Server.ServerWide.Commands.CompareExchangeCommandBase;

namespace Raven.Server.Storage.Schema.Updates.Server
{
    public sealed unsafe class From42012 : ISchemaUpdate
    {
        public int From => 42_012;

        public int To => 42_013;

        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Server;

        public bool Update(UpdateStep step)
        {
            var dbs = new List<string>();
            const string dbKey = "db/";

            var identities = step.ReadTx.ReadTree(ClusterStateMachine.Identities);
            step.WriteTx.DeleteTree(ClusterStateMachine.Identities);

            using (Slice.From(step.ReadTx.Allocator, dbKey, out Slice loweredPrefix))
            {
                var items = step.ReadTx.OpenTable(ClusterStateMachine.ItemsSchema, ClusterStateMachine.Items);
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
                {
                    dbs.Add(ClusterStateMachine.GetCurrentItemKey(result.Value).Substring(dbKey.Length));
                }
            }

            step.WriteTx.CreateTree(ClusterStateMachine.CompareExchangeIndex);

            foreach (var db in dbs)
            {
                if (identities != null)
                {
                    Slice.From(step.WriteTx.Allocator, "Identities", out var identitySlice);
                    ClusterStateMachine.IdentitiesSchema.Create(step.WriteTx, identitySlice, 32);
                    var writeTable = step.WriteTx.OpenTable(ClusterStateMachine.IdentitiesSchema, identitySlice);
                    using (Slice.From(step.ReadTx.Allocator, $"{dbKey}{db.ToLowerInvariant()}/identities/", out var identityPrefix))
                    {
                        using (var it = identities.Iterate(prefetch: false))
                        {
                            it.SetRequiredPrefix(identityPrefix);

                            if (it.Seek(identityPrefix))
                            {
                                do
                                {
                                    var key = it.CurrentKey;
                                    var keyAsString = key.ToString();   // old identity key
                                    var value = it.CreateReaderForCurrent().Read<long>();

                                    var newKey = keyAsString.Substring(identityPrefix.ToString().Length);

                                    // write to new identities schema
                                    GetKeyAndPrefixIndexSlices(step.ReadTx.Allocator, db, $"{newKey}", 0L, out var keyTuple, out var indexTuple);
                                    using (keyTuple.Scope)
                                    using (indexTuple.Scope)
                                    using (Slice.External(step.ReadTx.Allocator, keyTuple.Buffer.Ptr, keyTuple.Buffer.Length, out var keySlice))
                                    using (Slice.External(step.ReadTx.Allocator, indexTuple.Buffer.Ptr, indexTuple.Buffer.Length, out var prefixIndexSlice))
                                    {
                                        using (writeTable.Allocate(out var write))
                                        {
                                            write.Add(keySlice);
                                            write.Add(value);
                                            write.Add(0L);
                                            write.Add(prefixIndexSlice);

                                            writeTable.Set(write);
                                        }
                                    }
                                } while (it.MoveNext());
                            }
                        }
                    }
                }

                // update db backup status
                var dbLower = db.ToLowerInvariant();
                var items = step.WriteTx.OpenTable(ClusterStateMachine.ItemsSchema, ClusterStateMachine.Items);

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
