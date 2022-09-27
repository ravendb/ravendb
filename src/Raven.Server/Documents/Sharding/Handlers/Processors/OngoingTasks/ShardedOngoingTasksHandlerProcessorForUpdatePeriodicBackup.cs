using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForUpdatePeriodicBackup : AbstractOngoingTasksHandlerProcessorForUpdatePeriodicBackup<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedOngoingTasksHandlerProcessorForUpdatePeriodicBackup([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void OnBeforeUpdateConfiguration(ref PeriodicBackupConfiguration configuration, JsonOperationContext context)
        {
            if (configuration.BackupType == BackupType.Snapshot)
                throw new NotSupportedInShardingException($"Backups of type '{nameof(BackupType.Snapshot)}' are not supported in sharding.");

            base.OnBeforeUpdateConfiguration(ref configuration, context);
        }
    }
}
