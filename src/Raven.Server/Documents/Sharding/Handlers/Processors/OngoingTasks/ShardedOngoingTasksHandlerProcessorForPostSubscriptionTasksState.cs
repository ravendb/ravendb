using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForPostSubscriptionTasksState : AbstractOngoingTasksHandlerProcessorForPostSubscriptionTasksState<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedOngoingTasksHandlerProcessorForPostSubscriptionTasksState([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask ToggleSubscriptionTaskStateAsync()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForToggleTaskState(RequestHandler))
                await processor.ExecuteAsync();
        }
    }
}
