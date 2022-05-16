using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal class SubscriptionHandlerProcessorForDeleteSubscription : AbstractSubscriptionHandlerProcessorForDeleteSubscription<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public SubscriptionHandlerProcessorForDeleteSubscription([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override void RaiseNotificationForTaskRemoved(string subscriptionName)
        {
            RequestHandler.Database.SubscriptionStorage.RaiseNotificationForTaskRemoved(subscriptionName);
        }
    }
}
