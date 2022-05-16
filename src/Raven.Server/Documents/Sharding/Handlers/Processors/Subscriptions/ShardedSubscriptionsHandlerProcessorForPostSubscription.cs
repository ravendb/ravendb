using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Handlers.Processors.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Subscriptions
{
    internal class ShardedSubscriptionsHandlerProcessorForPostSubscription : AbstractSubscriptionsHandlerProcessorForPostSubscription<ShardedSubscriptionsHandler, TransactionOperationContext>
    {
        public ShardedSubscriptionsHandlerProcessorForPostSubscription([NotNull] ShardedSubscriptionsHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask CreateSubscriptionInternalAsync(BlittableJsonReaderObject bjro, long? id, bool? disabled, SubscriptionCreationOptions options, TransactionOperationContext context)
        {
            await RequestHandler.CreateSubscriptionInternalAsync(bjro, id, disabled, options, context);
        }
    }
}
