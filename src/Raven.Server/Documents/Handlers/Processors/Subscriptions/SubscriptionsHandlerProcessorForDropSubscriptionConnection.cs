using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal class SubscriptionsHandlerProcessorForDropSubscriptionConnection : AbstractSubscriptionsHandlerProcessorForDropSubscriptionConnection<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public SubscriptionsHandlerProcessorForDropSubscriptionConnection([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask DropSubscriptionAsync(long? subscriptionId, string subscriptionName, string workerId)
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var subscription = RequestHandler.Database
                    .SubscriptionStorage
                    .GetRunningSubscription(context, subscriptionId, subscriptionName, false);

                if (subscription != null)
                {
                    bool result;
                    if (string.IsNullOrEmpty(workerId) == false)
                    {
                        result = RequestHandler.Database.SubscriptionStorage.DropSingleSubscriptionConnection(subscription.SubscriptionId, workerId,
                            new SubscriptionClosedException(
                                $"Connection with Id {workerId} dropped by API request (request ip:{HttpContext.Connection.RemoteIpAddress}, cert:{HttpContext.Connection.ClientCertificate?.Thumbprint})",
                                canReconnect: false));
                    }
                    else
                    {
                        result = RequestHandler.Database.SubscriptionStorage.DropSubscriptionConnections(subscription.SubscriptionId,
                            new SubscriptionClosedException(
                                $"Dropped by API request (request ip:{HttpContext.Connection.RemoteIpAddress}, cert:{HttpContext.Connection.ClientCertificate?.Thumbprint})",
                                canReconnect: false));
                    }

                    if (result == false)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return;
                    }
                }
            }

            await RequestHandler.NoContent();
        }
    }
}
