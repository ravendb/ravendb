using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal class SubscriptionsHandlerProcessorForPostSubscription : AbstractSubscriptionsHandlerProcessorForPostSubscription<SubscriptionsHandler, DocumentsOperationContext, SubscriptionConnectionsState>
    {
        public SubscriptionsHandlerProcessorForPostSubscription([NotNull] SubscriptionsHandler requestHandler)
            : base(requestHandler, requestHandler.Database.SubscriptionStorage)
        {
        }

        protected override async ValueTask CreateSubscriptionInternalAsync(BlittableJsonReaderObject bjro, long? id, bool? disabled, SubscriptionCreationOptions options, ClusterOperationContext _)
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var sub = ParseSubscriptionQuery(options.Query);
                await RequestHandler.CreateInternalAsync(bjro, options, context, id, disabled, sub);
            }
        }
    }
}
