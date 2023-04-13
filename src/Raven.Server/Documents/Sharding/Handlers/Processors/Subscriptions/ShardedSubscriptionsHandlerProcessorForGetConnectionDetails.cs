using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Subscriptions
{
    internal class ShardedSubscriptionsHandlerProcessorForGetConnectionDetails : AbstractSubscriptionsHandlerProcessorForGetConnectionDetails<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedSubscriptionsHandlerProcessorForGetConnectionDetails([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override SubscriptionConnectionsDetails GetConnectionDetails(TransactionOperationContext context, string subscriptionName)
        {
            var state = RequestHandler.DatabaseContext.SubscriptionsStorage.GetSubscriptionConnectionsState(context, subscriptionName);
            return state?.GetSubscriptionConnectionsDetails();
        }
    }
}
