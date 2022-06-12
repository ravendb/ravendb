using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForBackupDatabaseNow : AbstractOngoingTasksHandlerProcessorForBackupDatabaseNow<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedOngoingTasksHandlerProcessorForBackupDatabaseNow([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ValueTask<bool> ScheduleBackupOperation(long taskId, bool isFullBackup, long operationId)
        {
            var token = RequestHandler.CreateTimeLimitedOperationToken();

            var t = RequestHandler.DatabaseContext.Operations.AddRemoteOperation<OperationIdResult<StartBackupOperationResult>, ShardedBackupResult>(operationId,
                Server.Documents.Operations.OperationType.DatabaseBackup,
                "Backup of sharded database : " + RequestHandler.DatabaseName,
                detailedDescription: null,
                (_, shardNumber) => new StartBackupOperation.StartBackupCommand(isFullBackup, taskId, operationId),
                token);

            _ = t.ContinueWith(_ =>
            {
                token.Dispose();
            });

            return ValueTask.FromResult(true);
        }

        protected override long GetNextOperationId()
        {
            return RequestHandler.DatabaseContext.Operations.GetNextOperationId();
        }
    }
}
