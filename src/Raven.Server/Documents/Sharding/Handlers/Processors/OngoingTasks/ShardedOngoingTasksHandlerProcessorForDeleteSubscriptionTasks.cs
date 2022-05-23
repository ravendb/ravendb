using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForDeleteSubscriptionTasks : AbstractOngoingTasksHandlerProcessorForDeleteSubscriptionTasks<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedOngoingTasksHandlerProcessorForDeleteSubscriptionTasks([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask DeleteOngoingTaskAsync()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForDeleteOngoingTask(RequestHandler))
                await processor.ExecuteAsync();
        }
    }
}
