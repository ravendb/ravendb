using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal sealed class SubscriptionsHandlerProcessorForDropSubscriptionConnection : AbstractSubscriptionsHandlerProcessorForDropSubscriptionConnection<DatabaseRequestHandler, DocumentsOperationContext, SubscriptionConnectionsState>
    {
        public SubscriptionsHandlerProcessorForDropSubscriptionConnection([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.Database.SubscriptionStorage)
        {
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token)
        {
            return RequestHandler.ExecuteRemoteAsync(command, token.Token);
        }
    }
}
