using JetBrains.Annotations;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal sealed class SubscriptionsHandlerProcessorForGetSubscriptionState : AbstractSubscriptionsHandlerProcessorForGetSubscriptionState<DatabaseRequestHandler, DocumentsOperationContext, SubscriptionConnectionsState>
    {
        public SubscriptionsHandlerProcessorForGetSubscriptionState([NotNull] DatabaseRequestHandler requestHandler) 
            : base(requestHandler, requestHandler.Database.SubscriptionStorage)
        {
        }
    }
}
