using System;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal abstract class AbstractSubscriptionsHandlerProcessorForDropSubscriptionConnection<TRequestHandler, TOperationContext, TSubscriptionState> : AbstractHandlerProxyNoContentProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TSubscriptionState : AbstractSubscriptionConnectionsState
    {
        protected readonly AbstractSubscriptionStorage<TSubscriptionState> SubscriptionStorage;


        protected AbstractSubscriptionsHandlerProcessorForDropSubscriptionConnection([NotNull] TRequestHandler requestHandler, [NotNull] AbstractSubscriptionStorage<TSubscriptionState> subscriptionStorage) : base(requestHandler)
        {
            SubscriptionStorage = subscriptionStorage ?? throw new ArgumentNullException(nameof(subscriptionStorage));
        }

        private async ValueTask DropSubscriptionAsync(long? subscriptionId, string subscriptionName, string workerId)
        {
            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                var subscription = SubscriptionStorage.GetActiveSubscription(context, subscriptionId, subscriptionName);

                if (subscription != null)
                {
                    var result = DropSingleSubscriptionConnection(subscription, string.IsNullOrEmpty(workerId) == false ? workerId : null);

                    if (result == false)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return;
                    }
                }
            }

            await RequestHandler.NoContent();
        }

        private bool DropSingleSubscriptionConnection(SubscriptionState subscription, string workerId)
        {
            bool result;
            if (workerId == null)
            {
                result = SubscriptionStorage.DropSubscriptionConnections(subscription.SubscriptionId,
                    new SubscriptionClosedException(
                        $"Dropped by API request (request ip:{HttpContext.Connection.RemoteIpAddress}, cert:{HttpContext.Connection.ClientCertificate?.Thumbprint})",
                        canReconnect: false));
            }
            else
            {
                result = SubscriptionStorage.DropSingleSubscriptionConnection(subscription.SubscriptionId, workerId,
                    new SubscriptionClosedException(
                        $"Connection with Id {workerId} dropped by API request (request ip:{HttpContext.Connection.RemoteIpAddress}, cert:{HttpContext.Connection.ClientCertificate?.Thumbprint})",
                        canReconnect: false));
            }

            return result;
        }

        protected override RavenCommand<object> CreateCommandForNode(string nodeTag)
        {
            return new DropSubscriptionConnectionCommand(SubscriptionName, SubscriptionId, WorkerId);
        }

        protected override bool SupportsCurrentNode => true;
        private long? SubscriptionId => RequestHandler.GetLongQueryString("id", required: false);
        private string SubscriptionName => RequestHandler.GetStringQueryString("name", required: false);
        private string WorkerId => RequestHandler.GetStringQueryString("workerId", required: false);

        protected override async ValueTask HandleCurrentNodeAsync()
        {
            await DropSubscriptionAsync(SubscriptionId, SubscriptionName, WorkerId);
        }
    }
}
