using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForResetEtl : AbstractOngoingTasksHandlerProcessorForResetEtl<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedOngoingTasksHandlerProcessorForResetEtl([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
