using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Web.System.Processors.OngoingTasks
{
    internal sealed class OngoingTasksHandlerProcessorForGetOngoingTask : AbstractOngoingTasksHandlerProcessorForGetOngoingTask<DatabaseRequestHandler, DocumentsOperationContext, SubscriptionConnectionsState>
    {
        public OngoingTasksHandlerProcessorForGetOngoingTask([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.Database.OngoingTasks)
        {
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<OngoingTask> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
    }
}
