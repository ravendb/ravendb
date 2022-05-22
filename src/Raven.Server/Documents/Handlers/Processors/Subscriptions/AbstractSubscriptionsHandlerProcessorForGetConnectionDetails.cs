using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal abstract class AbstractSubscriptionsHandlerProcessorForGetConnectionDetails<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        public AbstractSubscriptionsHandlerProcessorForGetConnectionDetails([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract SubscriptionConnectionsDetails GetConnectionDetails(TransactionOperationContext context, string subscriptionName);

        public override async ValueTask ExecuteAsync()
        {
            var subscriptionName = RequestHandler.GetStringQueryString("name", false);

            if (string.IsNullOrEmpty(subscriptionName))
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                return;
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var details = GetConnectionDetails(context, subscriptionName);

                if (details == null)
                    return;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, details.ToJson());
                }
            }
        }
    }
}
