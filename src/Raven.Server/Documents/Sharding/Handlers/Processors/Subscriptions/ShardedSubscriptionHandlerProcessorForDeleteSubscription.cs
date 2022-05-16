using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Subscriptions;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Subscriptions
{
    internal class ShardedSubscriptionHandlerProcessorForDeleteSubscription : AbstractSubscriptionHandlerProcessorForDeleteSubscription<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedSubscriptionHandlerProcessorForDeleteSubscription([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override void RaiseNotificationForTaskRemoved(string subscriptionName)
        {
        }
    }
}
