using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Handlers.Processors.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Subscriptions
{
    internal class ShardedSubscriptionsHandlerProcessorForPutSubscription : AbstractSubscriptionsHandlerProcessorForPutSubscription<ShardedSubscriptionsHandler, TransactionOperationContext>
    {
        public ShardedSubscriptionsHandlerProcessorForPutSubscription([NotNull] ShardedSubscriptionsHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask CreateInternalAsync(BlittableJsonReaderObject bjro, SubscriptionCreationOptions options, TransactionOperationContext context, long? id, bool? disabled)
        {
            using (context.OpenReadTransaction())
            {
                await RequestHandler.CreateSubscriptionInternalAsync(bjro, id, disabled, options, context);
            }
        }
    }
}
