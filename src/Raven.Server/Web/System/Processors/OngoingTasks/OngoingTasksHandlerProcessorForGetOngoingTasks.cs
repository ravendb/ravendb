using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Web.System.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForGetOngoingTasks : AbstractOngoingTasksHandlerProcessorForGetOngoingTasks<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public OngoingTasksHandlerProcessorForGetOngoingTasks([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.Database.OngoingTasks)
        {
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<OngoingTasksResult> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);

        protected override long SubscriptionsCount => RequestHandler.Database.SubscriptionStorage.GetAllSubscriptionsCount();
    }
}
