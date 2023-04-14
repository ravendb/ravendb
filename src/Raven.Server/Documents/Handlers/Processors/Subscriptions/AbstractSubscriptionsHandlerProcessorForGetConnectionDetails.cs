using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal abstract class AbstractSubscriptionsHandlerProcessorForGetConnectionDetails<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<SubscriptionConnectionsDetails, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractSubscriptionsHandlerProcessorForGetConnectionDetails([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected abstract SubscriptionConnectionsDetails GetConnectionDetails(TransactionOperationContext context, string subscriptionName);

        protected string GetName() => RequestHandler.GetStringQueryString("name");

        protected override RavenCommand<SubscriptionConnectionsDetails> CreateCommandForNode(string nodeTag)
        {
            var name = GetName();
            return new GetSubscriptionConnectionsDetailsCommand(name, nodeTag);
        }

        protected override async ValueTask HandleCurrentNodeAsync()
        {
            var subscriptionName = GetName();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var details = GetConnectionDetails(context, subscriptionName) ?? new SubscriptionConnectionsDetails()
                {
                    Results = new List<SubscriptionConnectionDetails>(),
                    SubscriptionMode = SubscriptionMode.None
                };

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, details.ToJson());
                }
            }
        }
    }
}
