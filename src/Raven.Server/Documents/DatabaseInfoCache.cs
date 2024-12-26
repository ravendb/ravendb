using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Json.Serialization;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents
{
    public class DatabaseInfoCache
    {

        protected readonly Logger Logger;

        private StorageEnvironment _environment;

        private TransactionContextPool _contextPool;

        private readonly TableSchema _databaseInfoSchema = new TableSchema();

        public DatabaseInfoCache()
        {
            Logger = LoggingSource.Instance.GetLogger<DatabaseInfoCache>("Server");
            _databaseInfoSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1
            });
        }

        public void Initialize(StorageEnvironment environment, TransactionContextPool contextPool)
        {
            _environment = environment;
            _contextPool = contextPool;

            using (contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = _environment.WriteTransaction(context.PersistentContext))
            {
                _databaseInfoSchema.Create(tx, DatabaseInfoSchema.DatabaseInfoTree, 16);

                tx.Commit();
            }
        }

        public unsafe void InsertDatabaseInfo(DynamicJsonValue databaseInfo, string databaseName)
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (var id = context.GetLazyString(databaseName.ToLowerInvariant()))
                using (var json = context.ReadObject(databaseInfo, "DatabaseInfo", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    using (var tx = context.OpenWriteTransaction(TimeSpan.FromSeconds(5)))
                    {
                        var table = tx.InnerTransaction.OpenTable(_databaseInfoSchema, DatabaseInfoSchema.DatabaseInfoTree);
                        using (table.Allocate(out TableValueBuilder tvb))
                        {
                            tvb.Add(id.Buffer, id.Size);
                            tvb.Add(json.BasePointer, json.Size);

                            table.Set(tvb);
                        }
                        tx.Commit();
                    }
                }
            }
        }

        public void InsertBackupStatus(PeriodicBackupStatus backupStatus, string databaseName, string dbId, long taskId)
        {
            var status = backupStatus.ToJson();
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenWriteTransaction(TimeSpan.FromSeconds(5)))
            {
                var statusBlittable = context.ReadObject(status, "BackupStatus", BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                InsertBackupStatusBlittable(context, statusBlittable, databaseName, dbId, taskId);
                tx.Commit();
            }
        }
        //TODO stav: create backup storage for backup status with dedicated tree. class will contain both history and statue storage
        public unsafe void InsertBackupStatusBlittable<T>(TransactionOperationContext<T> context, BlittableJsonReaderObject backupStatus, string databaseName, string dbId, long taskId) 
            where T : RavenTransaction
        {
            var key = PeriodicBackupStatus.GenerateItemName(databaseName, dbId, taskId);
            using (var id = context.GetLazyString(key.ToLowerInvariant()))
            using (backupStatus)
            {
                var table = context.Transaction.InnerTransaction.OpenTable(_databaseInfoSchema, DatabaseInfoSchema.DatabaseInfoTree);
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(id.Buffer, id.Size);
                    tvb.Add(backupStatus.BasePointer, backupStatus.Size);

                    table.Set(tvb);
                }
            }
        }

        public BlittableJsonReaderObject GetBackupStatusBlittable<T>(TransactionOperationContext<T> context, string databaseName, string dbId, long taskId) where T : RavenTransaction
        {
            var key = PeriodicBackupStatus.GenerateItemName(databaseName, dbId, taskId);
            var table = context.Transaction.InnerTransaction.OpenTable(_databaseInfoSchema, DatabaseInfoSchema.DatabaseInfoTree);

            TableValueReader infoTvr;
            using (Slice.From(context.Transaction.InnerTransaction.Allocator, key.ToLowerInvariant(), out Slice databaseNameAsSlice))
            {
                if (table.ReadByKey(databaseNameAsSlice, out infoTvr) == false)
                    return null;
            }

            unsafe
            {
                //it seems like the database was shutdown rudely and never wrote it stats onto the disk
                if (infoTvr.Pointer == null)
                    return null;
            }

            var statusBlittable = Read(context, ref infoTvr);
            return statusBlittable;
        }

        public PeriodicBackupStatus GetBackupStatus(string databaseName, string dbId, long taskId, TransactionOperationContext context)
        {
            PeriodicBackupStatus periodicBackup = null;
            using (var backupStatusBlittable = GetBackupStatusBlittable(context, databaseName, dbId, taskId))
            {
                if (backupStatusBlittable == null)
                    return null;

                periodicBackup = JsonDeserializationClient.PeriodicBackupStatus(backupStatusBlittable);
            }
            return periodicBackup;
        }

        public bool DeleteBackupStatusesByTaskIds(string databaseName, string dbId, HashSet<long> taskIds)
        {
            try
            {
                using (_contextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (var tx = ctx.OpenWriteTransaction(TimeSpan.FromSeconds(5)))
                {
                    foreach (var taskId in taskIds)
                    {
                        var backupKey = PeriodicBackupStatus.GenerateItemName(databaseName, dbId, taskId);
                        using (Slice.From(ctx.Allocator, backupKey.ToLowerInvariant(), out Slice key))
                        {
                            DeleteInternal(ctx, key);
                        }
                    }

                    tx.Commit();

                    var status = GetBackupStatus(databaseName, dbId, taskIds.First(), ctx);
                }

                if (Logger.IsInfoEnabled)
                    Logger.Info($"{databaseName}: Deleted local backup statuses for the following ids [{string.Join(",", taskIds)}], because node with db id {dbId} is not responsible anymore and is overdue for a full backup.");
            }
            catch (Exception ex)
            {
                if(Logger.IsInfoEnabled)
                    Logger.Info($"{databaseName}: Could not delete the local backup statuses for the following ids [{string.Join(",", taskIds)}]. We will not remove any tombstones.", ex);
                
                return false;
            }

            return true;
        }

        public void DeleteBackupStatus(ClusterOperationContext context, string databaseName, string dbId, long taskId)
        {
            // this is called from csm, so commiting will be done outside
            var backupKey = PeriodicBackupStatus.GenerateItemName(databaseName, dbId, taskId);
            using (Slice.From(context.Allocator, backupKey.ToLowerInvariant(), out Slice key))
            {
                var table = context.Transaction.InnerTransaction.OpenTable(_databaseInfoSchema, DatabaseInfoSchema.DatabaseInfoTree);
                table.DeleteByKey(key);
            }
        }

        public bool TryGet(string databaseName, Action<BlittableJsonReaderObject> action)
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_databaseInfoSchema, DatabaseInfoSchema.DatabaseInfoTree);

                TableValueReader infoTvr;
                using (Slice.From(tx.InnerTransaction.Allocator, databaseName.ToLowerInvariant(), out Slice databaseNameAsSlice))
                {
                    if (table.ReadByKey(databaseNameAsSlice, out infoTvr) == false)
                        return false;
                }

                unsafe
                {
                    //it seems like the database was shutdown rudely and never wrote it stats onto the disk
                    if (infoTvr.Pointer == null)
                        return false;
                }

                using (var databaseInfoJson = Read(context, ref infoTvr))
                {
                    action(databaseInfoJson);
                    return true;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe BlittableJsonReaderObject Read(JsonOperationContext context, ref TableValueReader reader)
        {
            var ptr = reader.Read(DatabaseInfoSchema.DatabaseInfoTable.JsonIndex, out int size);
            return new BlittableJsonReaderObject(ptr, size, context);
        }

        /// <summary>
        /// This method deletes the database info from the cache when the database is deleted.
        /// It assumes that the ctx already opened a write transaction.
        /// </summary>
        /// <param name="ctx">A context allocated outside the method with an open write transaction</param>
        /// <param name="databaseName">The database name as a slice</param>
        private void DeleteInternal(TransactionOperationContext ctx, Slice databaseName)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Deleting database info for '{databaseName}'.");
            var table = ctx.Transaction.InnerTransaction.OpenTable(_databaseInfoSchema, DatabaseInfoSchema.DatabaseInfoTree);
            table.DeleteByKey(databaseName);
        }

        public void Delete(string databaseName)
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (var tx = ctx.OpenWriteTransaction(TimeSpan.FromSeconds(5)))
            using (Slice.From(ctx.Allocator, databaseName.ToLowerInvariant(), out Slice key))
            {
                DeleteInternal(ctx, key);
                tx.Commit();
            }
        }

        public static class DatabaseInfoSchema
        {
            public const string DatabaseInfoTree = "DatabaseInfo";

            public static class DatabaseInfoTable
            {
#pragma warning disable 169
                public const int IdIndex = 0;
                public const int JsonIndex = 1;
#pragma warning restore 169
            }
        }


    }
}
