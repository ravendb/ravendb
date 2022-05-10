using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal class SubscriptionsHandlerProcessorForPutSubscription : AbstractSubscriptionsHandlerProcessorForPutSubscription<SubscriptionsHandler, DocumentsOperationContext>
    {
        public SubscriptionsHandlerProcessorForPutSubscription([NotNull] SubscriptionsHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask CreateInternalAsync(BlittableJsonReaderObject bjro, SubscriptionCreationOptions options, DocumentsOperationContext context, long? id, bool? disabled)
        {
            using (context.OpenReadTransaction())
            {
                await RequestHandler.CreateInternalAsync(bjro, options, context, id, disabled);
            }
        }
    }
}
