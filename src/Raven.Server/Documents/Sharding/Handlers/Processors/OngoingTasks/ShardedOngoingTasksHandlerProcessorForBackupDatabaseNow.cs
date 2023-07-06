using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForBackupDatabaseNow : AbstractOngoingTasksHandlerProcessorForBackupDatabaseNow<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedOngoingTasksHandlerProcessorForBackupDatabaseNow([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ValueTask<bool> ScheduleBackupOperationAsync(long taskId, bool isFullBackup, long operationId, DateTime? _)
        {
            var token = new OperationCancelToken(RequestHandler.DatabaseContext.Configuration.Databases.OperationTimeout.AsTimeSpan, RequestHandler.DatabaseContext.DatabaseShutdown);

            var startTime = SystemTime.UtcNow;
            var t = RequestHandler.DatabaseContext.Operations.AddRemoteOperation<OperationIdResult<StartBackupOperationResult>, ShardedBackupResult, ShardedBackupProgress>(operationId,
                Server.Documents.Operations.OperationType.DatabaseBackup,
                "Backup of sharded database : " + RequestHandler.DatabaseName,
                detailedDescription: null,
                (_, shardNumber) => new StartBackupOperation.StartBackupCommand(isFullBackup, taskId, operationId, startTime),
                token);

            t.ContinueWith(_ =>
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
