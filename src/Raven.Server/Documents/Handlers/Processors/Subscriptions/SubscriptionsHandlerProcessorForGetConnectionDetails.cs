using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal class SubscriptionsHandlerProcessorForGetConnectionDetails : AbstractSubscriptionsHandlerProcessorForGetConnectionDetails<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public SubscriptionsHandlerProcessorForGetConnectionDetails([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override SubscriptionConnectionsDetails GetConnectionDetails(TransactionOperationContext context, string subscriptionName)
        {
            var state = RequestHandler.Database.SubscriptionStorage.GetSubscriptionConnectionsState(context, subscriptionName);
            return state?.GetSubscriptionConnectionsDetails();
        }
    }
}
