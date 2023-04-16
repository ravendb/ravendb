using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Web.System.Processors.OngoingTasks;

internal abstract class OngoingTasksHandlerProcessorForGetOngoingTasksInfo : AbstractOngoingTasksHandlerProcessorForGetOngoingTasks<DatabaseRequestHandler, DocumentsOperationContext>
{
    protected OngoingTasksHandlerProcessorForGetOngoingTasksInfo([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.Database.OngoingTasks)
    {
    }
    protected override long SubscriptionsCount => (int)RequestHandler.Database.SubscriptionStorage.GetAllSubscriptionsCount();
}
