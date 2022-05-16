using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal abstract class AbstractSubscriptionsHandlerProcessorForGetSubscriptionState<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        public AbstractSubscriptionsHandlerProcessorForGetSubscriptionState([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            var subscriptionName = RequestHandler.GetStringQueryString("name", false);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                if (string.IsNullOrEmpty(subscriptionName))
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                    return;
                }
                
                var subscriptionState = RequestHandler.ServerStore.Cluster.Subscriptions.ReadSubscriptionStateByName(context, RequestHandler.DatabaseName, subscriptionName);
                
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
