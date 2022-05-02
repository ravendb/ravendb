using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForUpdatePeriodicBackup : AbstractOngoingTasksHandlerProcessorForUpdatePeriodicBackup<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedOngoingTasksHandlerProcessorForUpdatePeriodicBackup([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
