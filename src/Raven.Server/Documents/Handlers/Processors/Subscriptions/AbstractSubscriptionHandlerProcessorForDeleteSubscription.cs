using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Subscriptions;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal abstract class AbstractSubscriptionHandlerProcessorForDeleteSubscription<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        public AbstractSubscriptionHandlerProcessorForDeleteSubscription([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public abstract void RaiseNotificationForTaskRemoved(string subscriptionName);

        public override async ValueTask ExecuteAsync()
        {
            var subscriptionName = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("taskName");
            await SubscriptionStorage.DeleteSubscriptionInternal(ServerStore, RequestHandler.DatabaseName, subscriptionName, RequestHandler.GetRaftRequestIdFromQuery(), Logger);
            RaiseNotificationForTaskRemoved(subscriptionName);
            await RequestHandler.NoContent();
        }
    }
}
