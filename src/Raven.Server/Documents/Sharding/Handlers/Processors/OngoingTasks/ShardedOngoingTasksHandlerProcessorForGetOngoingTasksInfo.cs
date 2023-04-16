using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System.Processors.OngoingTasks;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks;

internal abstract class ShardedOngoingTasksHandlerProcessorForGetOngoingTasksInfo : AbstractOngoingTasksHandlerProcessorForGetOngoingTasks<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    protected ShardedOngoingTasksHandlerProcessorForGetOngoingTasksInfo([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.DatabaseContext.OngoingTasks)
    {
    }

    protected override long SubscriptionsCount => (int)RequestHandler.DatabaseContext.SubscriptionsStorage.GetAllSubscriptionsCount();
}
