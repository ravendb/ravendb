using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForBackupDatabaseOnce : AbstractOngoingTasksHandlerProcessorForBackupDatabaseOnce<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedOngoingTasksHandlerProcessorForBackupDatabaseOnce([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask ExecuteBackup(TransactionOperationContext context, BackupConfiguration backupConfiguration, long operationId)
        {
            var token = RequestHandler.CreateOperationToken();

            await RequestHandler.DatabaseContext.Operations.AddLocalOperation(operationId,
                Server.Documents.Operations.OperationType.DatabaseBackup,
                "One Time backup of database : " + RequestHandler.DatabaseName,
                detailedDescription: null,
                _ => BackupOnceOnAllShards(backupConfiguration),
                token);
        }

        protected override long GetNextOperationId()
        {
            return RequestHandler.DatabaseContext.Operations.GetNextOperationId();
        }

        private async Task<IOperationResult> BackupOnceOnAllShards(BackupConfiguration backupConfiguration)
        {
            var backupOperation = new ShardedBackupOnceOperation(RequestHandler, backupConfiguration);
            await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(backupOperation);
            await backupOperation.WaitForBackupToCompleteOnAllShards();

            var getStateOperation = new GetShardedOperationStateOperation(RequestHandler.HttpContext, backupOperation.OperationId);
            return (await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(getStateOperation)).Result;
        }
    }
}
