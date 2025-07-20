using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class BackupStatusStorage
    {
        private StorageEnvironment _environment;
        private TransactionContextPool _contextPool;

        private readonly Logger _logger = LoggingSource.Instance.GetLogger<BackupStatusStorage>("Server");

        private static readonly TableSchema BackupStatusTableSchema = new();

        private static readonly Slice BackupStatusSlice;

        private readonly ServerStore _serverStore;

        static BackupStatusStorage()
        {
            using (StorageEnvironment.GetStaticContext(out ByteStringContext ctx))
            {
                Slice.From(ctx, nameof(BackupStatusStorage), ByteStringType.Immutable, out BackupStatusSlice);
            }

            BackupStatusTableSchema.DefineKey(new TableSchema.IndexDef { StartIndex = 0, Count = 1 });
        }

        public BackupStatusStorage(ServerStore serverStore)
        {
            _serverStore = serverStore;
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

        public PeriodicBackupStatus GetBackupStatus(string databaseName, long taskId)
        {
            using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                return GetBackupStatus(context, databaseName, taskId);
            }
        }

        public static PeriodicBackupStatus GetBackupStatus(ClusterOperationContext context, string databaseName, long taskId)
        {
            var backupStatusBlittable = GetBackupStatusBlittable(context, databaseName, taskId);
            return backupStatusBlittable == null
                ? null
                : JsonDeserializationClient.PeriodicBackupStatus(backupStatusBlittable);
        }

        public static BlittableJsonReaderObject GetBackupStatusBlittable(ClusterOperationContext context, string databaseName, long taskId)
        {
            var statusBlittable = GetLocalBackupStatusBlittableInternal(context, databaseName, taskId);
            if (statusBlittable != null)
                return statusBlittable;

            // backwards compatibility - fallback to cluster status - only if the node tag matches
            var clusterStatus = BackupUtils.GetBackupStatusFromClusterBlittable(context, databaseName, taskId);
            if (clusterStatus == null || clusterStatus.TryGet(nameof(PeriodicBackupStatus.NodeTag), out string nodeTag) == false || string.IsNullOrEmpty(nodeTag))
                return null;

            if (nodeTag == RachisConsensus.ReadNodeTag(context))
                return clusterStatus;

            return null;
        }

        internal static unsafe BlittableJsonReaderObject GetLocalBackupStatusBlittableInternal<T>(TransactionOperationContext<T> context, string databaseName, long taskId)
            where T : RavenTransaction
        {
            var key = PeriodicBackupStatus.GenerateItemName(databaseName, taskId);
            var table = context.Transaction.InnerTransaction.OpenTable(BackupStatusTableSchema, BackupStatusSchema.TableName);

            TableValueReader statusTvr;
            using (Slice.From(context.Transaction.InnerTransaction.Allocator, key.ToLowerInvariant(), out Slice databaseNameAsSlice))
            {
                if (table.ReadByKey(databaseNameAsSlice, out statusTvr) == false)
                    return null;
            }

            //it seems like the database was shutdown rudely and never wrote its stats onto the disk
            if (statusTvr.Pointer == null)
                return null;

            var ptr = statusTvr.Read(BackupStatusSchema.BackupStatusColumns.Data, out int size);
            var statusBlittable = new BlittableJsonReaderObject(ptr, size, context);

            return statusBlittable;
        }

        internal Dictionary<long, PeriodicBackupStatusReport> GetDatabasePeriodicBackupStatuses(ClusterOperationContext context, string databaseName, List<long> backupTaskIds)
        {
            var result = new Dictionary<long, PeriodicBackupStatusReport>();

            foreach (var taskId in backupTaskIds)
            {
                using var statusBlittable = GetBackupStatusBlittable(context, databaseName, taskId);
                if (statusBlittable == null)
                    continue;

                var backupStatusReport = PeriodicBackupStatusReport.Deserialize(statusBlittable);
                if (backupStatusReport == null)
                    continue;

                result[taskId] = backupStatusReport;
            }

            return result;
        }

        public void Insert(PeriodicBackupStatus backupStatus, string databaseName)
        {
            _serverStore.Engine.TxMerger.EnqueueSync(new UpdateLocalBackupStatusCommand(backupStatus, databaseName, backupStatus.TaskId));

            if (_logger.IsInfoEnabled)
                _logger.Info($"Inserted backup status for database '{databaseName}' with task ID {backupStatus.TaskId}.");
        }

        public static unsafe void Insert<T>(TransactionOperationContext<T> context, BlittableJsonReaderObject backupStatus, string databaseName, long taskId)
            where T : RavenTransaction
        {
            var key = PeriodicBackupStatus.GenerateItemName(databaseName, taskId);
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

        public static void Delete(ClusterOperationContext context, string databaseName, long taskId)
        {
            // this is called from csm, so commiting will be done outside
            var backupKey = PeriodicBackupStatus.GenerateItemName(databaseName, taskId);
            using (Slice.From(context.Allocator, backupKey.ToLowerInvariant(), out Slice key))
            {
                var table = context.Transaction.InnerTransaction.OpenTable(BackupStatusTableSchema, BackupStatusSchema.TableName);
                table.DeleteByKey(key);
            }
        }

        internal void Delete(string databaseName)
        {
            var prefix = PeriodicBackupStatus.GenerateBackupStoragePrefix(databaseName);

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenWriteTransaction(TimeSpan.FromSeconds(5)))
            using (Slice.From(context.Allocator, prefix.ToLowerInvariant(), out Slice key))
            {
                var table = context.Transaction.InnerTransaction.OpenTable(BackupStatusTableSchema, BackupStatusSchema.TableName);
                table.DeleteByPrimaryKeyPrefix(key);
                tx.Commit();
            }
        }

        private static class BackupStatusSchema
        {
            public const string TableName = "BackupStatusTable";

            public static class BackupStatusColumns
            {
#pragma warning disable 169
                // Primary key structure: values/{databaseName}/periodic-backups/{taskId}
                public const int PrimaryKey = 0;
                public const int Data = 1;
#pragma warning restore 169
            }
        }
    }
}
