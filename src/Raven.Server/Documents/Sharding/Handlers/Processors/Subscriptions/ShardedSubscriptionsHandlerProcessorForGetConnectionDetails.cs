using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Subscriptions
{
    internal class ShardedSubscriptionsHandlerProcessorForGetConnectionDetails : AbstractSubscriptionsHandlerProcessorForGetConnectionDetails<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedSubscriptionsHandlerProcessorForGetConnectionDetails([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override SubscriptionConnectionsDetails GetConnectionDetails(ClusterOperationContext context, string subscriptionName)
        {
            var state = RequestHandler.DatabaseContext.SubscriptionsStorage.GetSubscriptionConnectionsState(context, subscriptionName);
            return state?.GetSubscriptionConnectionsDetails();
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<SubscriptionConnectionsDetails> command, OperationCancelToken token)
        {
            return TryGetShardNumber(out int shardNumber) 
                ? RequestHandler.DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token) 
                : RequestHandler.DatabaseContext.AllOrchestratorNodesExecutor.ExecuteForNodeAsync(command, command.SelectedNodeTag, token.Token);
        }
    }
}
