// -----------------------------------------------------------------------
//  <copyright file="ShardedOngoingTasksHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Replication;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedOngoingTasksHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/tasks", "GET")]
        public async Task GetOngoingTasks()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForGetOngoingTasks(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/connection-strings", "PUT")]
        public async Task PutConnectionString()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForPutConnectionString(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/connection-strings", "GET")]
        public async Task GetConnectionStrings()
        {
            using (var processor = new OngoingTasksHandlerProcessorForGetConnectionString<ShardedDatabaseRequestHandler, TransactionOperationContext>(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/connection-strings", "DELETE")]
        public async Task RemoveConnectionString()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForRemoveConnectionString(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/etl", "PUT")]
        public async Task AddEtl()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForAddEtl(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/etl", "RESET")]
        public async Task ResetEtl()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForResetEtl(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/subscription-tasks", "DELETE")]
        public async Task DeleteSubscriptionTask()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForDeleteSubscriptionTasks(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/tasks", "DELETE")]
        public async Task DeleteOngoingTask()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForDeleteOngoingTask(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/subscription-tasks/state", "POST")]
        public async Task ToggleSubscriptionTaskState()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForToggleTaskState(this, requireAdmin: false))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/tasks/state", "POST")]
        public async Task ToggleTaskState()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForToggleTaskState(this, requireAdmin: true))
                await processor.ExecuteAsync();
        }

        // Get Info about a specific task - For Edit View in studio - Each task should return its own specific object
        [RavenShardedAction("/databases/*/task", "GET")]
        public async Task GetOngoingTaskInfo()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForGetOngoingTaskInfo(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/periodic-backup", "POST")]
        public async Task UpdatePeriodicBackup()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForUpdatePeriodicBackup(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/periodic-backup/config", "GET")]
        public async Task GetConfiguration()
        {
            using (var processor = new OngoingTasksHandlerProcessorForGetPeriodicBackupConfiguration<ShardedDatabaseRequestHandler, TransactionOperationContext>(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/backup-data-directory", "GET")]
        public async Task FullBackupDataDirectory()
        {
            using (var processor = new OngoingTasksHandlerProcessorForGetFullBackupDataDirectory<ShardedDatabaseRequestHandler, TransactionOperationContext>(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/tasks/external-replication", "POST")]
        public async Task UpdateExternalReplication()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForUpdateExternalReplication(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/tasks/pull-replication/hub", "GET")]
        public async Task GetHubTasksInfo()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForGetPullReplicationHubTasksInfo(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/backup", "POST")]
        public async Task BackupDatabaseOnce()
        {
            var operationId = DatabaseContext.Operations.GetNextOperationId();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
                }

                var token = CreateOperationToken();

                await DatabaseContext.Operations.AddLocalOperation(operationId,
                    Documents.Operations.OperationType.DatabaseBackup,
                    "One Time backup of database : " + DatabaseName,
                    detailedDescription: null,
                    _ => BackupOnceOnAllShards(context),
                    token);
            }
        }

        [RavenShardedAction("/databases/*/admin/backup/database", "POST")]
        public async Task BackupDatabase()
        {
            var operationId = DatabaseContext.Operations.GetNextOperationId();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
                }
            }

            var token = CreateOperationToken();

            await DatabaseContext.Operations.AddLocalOperation(operationId,
                Documents.Operations.OperationType.DatabaseBackup,
                "One Time backup of database : " + DatabaseName,
                detailedDescription: null,
                _ => BackupNowOnAllShards(),
                token);
        }

        private async Task<IOperationResult> BackupNowOnAllShards()
        {
            var operationId = -DatabaseContext.Operations.GetNextOperationId();
            var backupNowOperation = new ShardedBackupNowOperation(this, operationId);
            await ShardExecutor.ExecuteParallelForAllAsync(backupNowOperation);

            await backupNowOperation.WaitForBackupToCompleteOnAllShards();

            var getStateOperation = new GetShardedOperationStateOperation(HttpContext, operationId);
            return (await ShardExecutor.ExecuteParallelForAllAsync(getStateOperation)).Result;
        }


        private async Task<IOperationResult> BackupOnceOnAllShards(JsonOperationContext context)
        {
            var operationId = -DatabaseContext.Operations.GetNextOperationId();
            var backupConfig = await context.ReadForMemoryAsync(RequestBodyStream(), "database-backup");

            var backupOnceOperation = new ShardedBackupOnceOperation(this, operationId, backupConfig);
            await ShardExecutor.ExecuteParallelForAllAsync(backupOnceOperation);

            await backupOnceOperation.WaitForBackupToCompleteOnAllShards();

            var getStateOperation = new GetShardedOperationStateOperation(HttpContext, operationId);
            return (await ShardExecutor.ExecuteParallelForAllAsync(getStateOperation)).Result;
        }
    }
}
