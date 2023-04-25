using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal abstract class AbstractSubscriptionsHandlerProcessorForGetSubscriptionState<TRequestHandler, TOperationContext, TSubscriptionState> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TSubscriptionState : AbstractSubscriptionConnectionsState
    {
        protected readonly AbstractSubscriptionStorage<TSubscriptionState> SubscriptionStorage;

        protected AbstractSubscriptionsHandlerProcessorForGetSubscriptionState([NotNull] TRequestHandler requestHandler, AbstractSubscriptionStorage<TSubscriptionState> subscriptionStorage) : base(requestHandler)
        {
            SubscriptionStorage = subscriptionStorage;
        }

        public override async ValueTask ExecuteAsync()
        {
            var subscriptionName = RequestHandler.GetStringQueryString("name", false);

            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                if (string.IsNullOrEmpty(subscriptionName))
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                    return;
                }

                var subscriptionState = SubscriptionStorage.GetSubscriptionByName(context, subscriptionName);

                if (subscriptionState == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                context.Write(writer, subscriptionState.ToJson());
            }
        }
    }
}
