using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Subscriptions;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Subscriptions
{
    internal sealed class ShardedSubscriptionsHandlerProcessorForDropSubscriptionConnection : AbstractSubscriptionsHandlerProcessorForDropSubscriptionConnection<ShardedDatabaseRequestHandler, TransactionOperationContext, SubscriptionConnectionsStateOrchestrator>
    {
        public ShardedSubscriptionsHandlerProcessorForDropSubscriptionConnection([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.DatabaseContext.SubscriptionsStorage)
        {
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token)
        {
            return RequestHandler.DatabaseContext.AllOrchestratorNodesExecutor.ExecuteForNodeAsync(command, command.SelectedNodeTag, token.Token);
        }
    }
}
