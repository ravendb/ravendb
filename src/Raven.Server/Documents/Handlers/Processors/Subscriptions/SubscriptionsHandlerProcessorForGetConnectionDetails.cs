using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal class SubscriptionsHandlerProcessorForGetConnectionDetails : AbstractSubscriptionsHandlerProcessorForGetConnectionDetails<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public SubscriptionsHandlerProcessorForGetConnectionDetails([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override SubscriptionConnectionsDetails GetConnectionDetails(ClusterOperationContext context, string subscriptionName)
        {
            var state = RequestHandler.Database.SubscriptionStorage.GetSubscriptionConnectionsState(context, subscriptionName);
            return state?.GetSubscriptionConnectionsDetails();
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<SubscriptionConnectionsDetails> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
    }
}
