using System.Diagnostics;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.Documents.Operations;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForBackupDatabaseOnce : AbstractOngoingTasksHandlerProcessorForBackupDatabaseOnce<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedOngoingTasksHandlerProcessorForBackupDatabaseOnce([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void ScheduleBackup(BackupConfiguration backupConfiguration, long operationId, string backupName, Stopwatch sw, OperationCancelToken token)
        {
            var t = RequestHandler.DatabaseContext.Operations.AddRemoteOperation<OperationIdResult<StartBackupOperationResult>, ShardedBackupResult, ShardedBackupProgress>(
                operationId,
                OperationType.DatabaseBackup,
                $"Manual backup for database: {RequestHandler.DatabaseName}",
                detailedDescription: null,
                commandFactory: (context, shardNumber) => new BackupOperation.BackupCommand(backupConfiguration, operationId),
                token);

            var _ = t.ContinueWith(_ =>
            {
                token.Dispose();
            });
        }

        protected override void AssertBackup(BackupConfiguration configuration)
        {
            if (configuration.BackupType == BackupType.Snapshot)
                throw new NotSupportedInShardingException($"Backups of type '{nameof(BackupType.Snapshot)}' are not supported in sharding.");

            base.AssertBackup(configuration);
        }

        protected override long GetNextOperationId()
        {
            return RequestHandler.DatabaseContext.Operations.GetNextOperationId();
        }

        protected override AbstractNotificationCenter GetNotificationCenter()
        {
            return RequestHandler.DatabaseContext.NotificationCenter;
        }
    }
}
