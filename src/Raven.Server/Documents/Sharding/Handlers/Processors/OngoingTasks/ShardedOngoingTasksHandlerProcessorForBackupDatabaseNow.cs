using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForBackupDatabaseNow : AbstractDatabaseHandlerProcessor<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedOngoingTasksHandlerProcessorForBackupDatabaseNow([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            var operationId = RequestHandler.DatabaseContext.Operations.GetNextOperationId();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
                }
            }

            var token = RequestHandler.CreateOperationToken();

            await RequestHandler.DatabaseContext.Operations.AddLocalOperation(operationId,
                Server.Documents.Operations.OperationType.DatabaseBackup,
                "Backup of sharded database : " + RequestHandler.DatabaseName,
                detailedDescription: null,
                _ => BackupNowOnAllShards(),
                token);
        }

        private async Task<IOperationResult> BackupNowOnAllShards()
        {
            var backupOperation = new ShardedBackupNowOperation(RequestHandler);

            await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(backupOperation);
            await backupOperation.WaitForBackupToCompleteOnAllShards();

            var getStateOperation = new GetShardedOperationStateOperation(RequestHandler.HttpContext, backupOperation.OperationId);
            return (await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(getStateOperation)).Result;
        }
    }
}
