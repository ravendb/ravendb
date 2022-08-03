using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForDeleteSubscriptionTasks : AbstractOngoingTasksHandlerProcessorForDeleteSubscriptionTasks<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedOngoingTasksHandlerProcessorForDeleteSubscriptionTasks([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask RaiseNotificationForSubscriptionTaskRemoval()
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal,
                "RavenDB-19068 Implement this correctly after it is implemented in ShardedOngoingTasksHandlerProcessorForDeleteOngoingTask");
            var tasks = RequestHandler.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(RequestHandler.DatabaseName);
            foreach (var task in tasks)
            {
                var db = await task;
                db.SubscriptionStorage.RaiseNotificationForTaskRemoved(TaskName);
            }
        }
    }
}
