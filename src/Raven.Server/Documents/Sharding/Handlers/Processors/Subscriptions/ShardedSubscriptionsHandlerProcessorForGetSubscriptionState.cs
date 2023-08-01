using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Subscriptions;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Subscriptions
{
    internal sealed class ShardedSubscriptionsHandlerProcessorForGetSubscriptionState : AbstractSubscriptionsHandlerProcessorForGetSubscriptionState<ShardedDatabaseRequestHandler, TransactionOperationContext, SubscriptionConnectionsStateOrchestrator>
    {
        public ShardedSubscriptionsHandlerProcessorForGetSubscriptionState([NotNull] ShardedDatabaseRequestHandler requestHandler) 
            : base(requestHandler, requestHandler.DatabaseContext.SubscriptionsStorage)
        {
        }
    }
}
