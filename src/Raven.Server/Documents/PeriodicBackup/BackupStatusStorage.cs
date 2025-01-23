using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class BackupStatusStorage
    {
        private const string JsonDocumentId = "backup-status-entry";

        private StorageEnvironment _environment;
        private TransactionContextPool _contextPool;

        private readonly Logger _logger = LoggingSource.Instance.GetLogger<BackupStatusStorage>("Server");

        private static readonly TableSchema BackupStatusTableSchema = new();

        private static readonly Slice BackupStatusSlice;

        public SystemTime Time = new SystemTime();

        static BackupStatusStorage()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "BackupStatus", ByteStringType.Immutable, out BackupStatusSlice);
            }

            BackupStatusTableSchema.DefineKey(new TableSchema.SchemaIndexDef { StartIndex = 0, Count = 1 });
        }

        public void Initialize(StorageEnvironment environment, TransactionContextPool contextPool)
        {
            _environment = environment;
            _contextPool = contextPool;

            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = _environment.WriteTransaction(context.PersistentContext))
            {
                tx.CreateTree(BackupStatusSlice);
                BackupStatusTableSchema.Create(tx, BackupStatusSchema.TableName, 16);
                tx.Commit();
            }
        }

        public static PeriodicBackupStatus GetBackupStatus(string databaseName, string dbId, long taskId, TransactionOperationContext context)
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

        public static unsafe BlittableJsonReaderObject GetBackupStatusBlittable<T>(TransactionOperationContext<T> context, string databaseName, string dbId, long taskId)
            where T : RavenTransaction
        {
            var key = PeriodicBackupStatus.GenerateItemName(databaseName, dbId, taskId);
            var table = context.Transaction.InnerTransaction.OpenTable(BackupStatusTableSchema, BackupStatusSchema.TableName);

            TableValueReader statusTvr;
            using (Slice.From(context.Transaction.InnerTransaction.Allocator, key.ToLowerInvariant(), out Slice databaseNameAsSlice))
            {
                if (table.ReadByKey(databaseNameAsSlice, out statusTvr) == false)
                    return null;
            }

            //it seems like the database was shutdown rudely and never wrote it stats onto the disk
            if (statusTvr.Pointer == null)
                return null;

            var ptr = statusTvr.Read(BackupStatusSchema.BackupStatusColumns.Data, out int size);
            var statusBlittable = new BlittableJsonReaderObject(ptr, size, context);

            return statusBlittable;
        }

        public void InsertBackupStatus(PeriodicBackupStatus backupStatus, string databaseName, string dbId, long taskId)
        {
            var status = backupStatus.ToJson();
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenWriteTransaction(TimeSpan.FromMinutes(1)))
            {
                var statusBlittable = context.ReadObject(status, JsonDocumentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                InsertBackupStatusBlittable(context, statusBlittable, databaseName, dbId, taskId);
                tx.Commit();
            }
        }

        public static unsafe void InsertBackupStatusBlittable<T>(TransactionOperationContext<T> context, BlittableJsonReaderObject backupStatus, string databaseName,
            string dbId, long taskId)
            where T : RavenTransaction
        {
            var key = PeriodicBackupStatus.GenerateItemName(databaseName, dbId, taskId);
            using (var id = context.GetLazyString(key.ToLowerInvariant()))
            using (backupStatus)
            {
                var table = context.Transaction.InnerTransaction.OpenTable(BackupStatusTableSchema, BackupStatusSchema.TableName);
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(id.Buffer, id.Size);
                    tvb.Add(backupStatus.BasePointer, backupStatus.Size);

                    table.Set(tvb);
                }
            }
        }

        public bool DeleteBackupStatusesByTaskIds(string databaseName, string dbId, HashSet<long> taskIds)
        {
            try
            {
                using (_contextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (var tx = ctx.OpenWriteTransaction(TimeSpan.FromMinutes(1)))
                {
                    foreach (var taskId in taskIds)
                    {
                        var backupKey = PeriodicBackupStatus.GenerateItemName(databaseName, dbId, taskId);
                        using (Slice.From(ctx.Allocator, backupKey.ToLowerInvariant(), out Slice key))
                        {
                            var table = ctx.Transaction.InnerTransaction.OpenTable(BackupStatusTableSchema, BackupStatusSchema.TableName);
                            table.DeleteByKey(key);
                        }
                    }

                    tx.Commit();
                }

                if (_logger.IsInfoEnabled)
                    _logger.Info(
                        $"{databaseName}: Deleted local backup statuses for the following ids [{string.Join(",", taskIds)}], because node with db id {dbId} is not responsible anymore and is overdue for a full backup.");
            }
            catch (Exception ex)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info(
                        $"{databaseName}: Could not delete the local backup statuses for the following ids [{string.Join(",", taskIds)}]. We will not remove any tombstones.",
                        ex);

                return false;
            }

            return true;
        }

        public static void DeleteBackupStatus(ClusterOperationContext context, string databaseName, string dbId, long taskId)
        {
            // this is called from csm, so commiting will be done outside
            Debug.Assert(context.Transaction?.InnerTransaction?.IsWriteTransaction == true);

            var backupKey = PeriodicBackupStatus.GenerateItemName(databaseName, dbId, taskId);
            using (Slice.From(context.Allocator, backupKey.ToLowerInvariant(), out Slice key))
            {
                var table = context.Transaction.InnerTransaction.OpenTable(BackupStatusTableSchema, BackupStatusSchema.TableName);
                table.DeleteByKey(key);
            }
        }

        private static class BackupStatusSchema
        {
            public const string TableName = "BackupStatusTable";
            private const string ValuesPrefix = "values/";

            public static class BackupStatusColumns

            {
#pragma warning disable 169
                public const int PrimaryKey = 0;
                public const int Data = 1;
#pragma warning restore 169
            }
        }

    }
}
