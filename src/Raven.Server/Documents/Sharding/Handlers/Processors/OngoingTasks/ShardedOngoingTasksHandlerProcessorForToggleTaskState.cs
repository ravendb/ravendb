using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForToggleTaskState : AbstractOngoingTasksHandlerProcessorForToggleTaskState<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedOngoingTasksHandlerProcessorForToggleTaskState([NotNull] ShardedDatabaseRequestHandler requestHandler, bool requireAdmin) : base(requestHandler)
        {
            RequireAdmin = requireAdmin;
        }

        protected override bool RequireAdmin { get; }
    }
}
