using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal class SubscriptionsHandlerProcessorForGetSubscriptionState : AbstractSubscriptionsHandlerProcessorForGetSubscriptionState<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public SubscriptionsHandlerProcessorForGetSubscriptionState([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
