using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal class SubscriptionsHandlerProcessorForPostSubscription : AbstractSubscriptionsHandlerProcessorForPostSubscription<SubscriptionsHandler, DocumentsOperationContext>
    {
        public SubscriptionsHandlerProcessorForPostSubscription([NotNull] SubscriptionsHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask CreateSubscriptionInternalAsync(BlittableJsonReaderObject bjro, long? id, bool? disabled, SubscriptionCreationOptions options, TransactionOperationContext _)
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                await RequestHandler.CreateInternalAsync(bjro, options, context, id, disabled);
            }
        }
    }
}
