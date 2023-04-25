using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal class SubscriptionsHandlerProcessorForGetResend : AbstractSubscriptionsHandlerProcessorForGetResend<DatabaseRequestHandler, DocumentsOperationContext, SubscriptionConnectionsState>
    {
        public SubscriptionsHandlerProcessorForGetResend([NotNull] DatabaseRequestHandler requestHandler) 
            : base(requestHandler, requestHandler.Database.SubscriptionStorage)
        {
        }

        protected override HashSet<long> GetActiveBatches(ClusterOperationContext context, SubscriptionState subscriptionState)
        {
            var subscriptionConnections = SubscriptionStorage.GetSubscriptionConnectionsState(context, subscriptionState.SubscriptionName);
            return subscriptionConnections?.GetActiveBatches();
        }
    }
}
