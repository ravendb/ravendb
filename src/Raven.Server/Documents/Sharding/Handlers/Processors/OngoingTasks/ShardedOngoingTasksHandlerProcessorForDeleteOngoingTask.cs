using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForDeleteOngoingTask : AbstractOngoingTasksHandlerProcessorForDeleteOngoingTask<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedOngoingTasksHandlerProcessorForDeleteOngoingTask([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask RaiseNotificationForSubscriptionTaskRemoval()
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Normal, "RavenDB-19068 how should we handle other nodes? ");
            var tasks = RequestHandler.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(RequestHandler.DatabaseName);
            foreach (var task in tasks)
            {
                var db = await task;
                db.SubscriptionStorage.RaiseNotificationForTaskRemoved(TaskName);
            }
        }
    }
}
