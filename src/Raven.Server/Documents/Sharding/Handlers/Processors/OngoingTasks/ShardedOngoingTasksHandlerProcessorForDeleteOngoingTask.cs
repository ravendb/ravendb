using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForDeleteOngoingTask : AbstractOngoingTasksHandlerProcessorForDeleteOngoingTask<ShardedDatabaseRequestHandler>
    {
        public ShardedOngoingTasksHandlerProcessorForDeleteOngoingTask([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override string GetDatabaseName()
        {
            return RequestHandler.DatabaseContext.DatabaseName;
        }

        protected override async ValueTask WaitForIndexNotificationAsync(long index)
        {
            await RequestHandler.DatabaseContext.Cluster.WaitForExecutionOnAllNodesAsync(index);
        }

        protected override async ValueTask RaiseNotificationForSubscriptionTaskRemoval()
        {
            var tasks = RequestHandler.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(RequestHandler.DatabaseContext.DatabaseName);
            foreach (var task in tasks)
            {
                var db = await task;
                db.SubscriptionStorage.RaiseNotificationForTaskRemoved(TaskName);
            }
        }
    }
}
