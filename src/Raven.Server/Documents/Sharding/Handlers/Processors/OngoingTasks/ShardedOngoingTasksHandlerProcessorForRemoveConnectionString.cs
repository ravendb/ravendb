using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForRemoveConnectionString : AbstractOngoingTasksHandlerProcessorForRemoveConnectionString<ShardedDatabaseRequestHandler>
    {
        public ShardedOngoingTasksHandlerProcessorForRemoveConnectionString([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
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
    }
}
