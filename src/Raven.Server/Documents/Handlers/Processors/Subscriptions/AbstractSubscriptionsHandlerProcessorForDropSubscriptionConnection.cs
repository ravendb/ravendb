using System.Threading.Tasks;
using JetBrains.Annotations;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal abstract class AbstractSubscriptionsHandlerProcessorForDropSubscriptionConnection<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        public AbstractSubscriptionsHandlerProcessorForDropSubscriptionConnection([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask DropSubscriptionAsync(long? subscriptionId, string subscriptionName, string workerId);

        public override async ValueTask ExecuteAsync()
        {
            var subscriptionId = RequestHandler.GetLongQueryString("id", required: false);
            var subscriptionName = RequestHandler.GetStringQueryString("name", required: false);
            var workerId = RequestHandler.GetStringQueryString("workerId", required: false);

            await DropSubscriptionAsync(subscriptionId, subscriptionName, workerId);
        }
    }
}
