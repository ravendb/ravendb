using System.Net;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Subscriptions;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Subscriptions
{
    internal class ShardedSubscriptionsHandlerProcessorForGetConnectionDetails : AbstractSubscriptionsHandlerProcessorForGetConnectionDetails<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedSubscriptionsHandlerProcessorForGetConnectionDetails([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override SubscriptionConnectionsDetails GetConnectionDetails(TransactionOperationContext context, string subscriptionName)
        {
            if (ShardedSubscriptionConnection.Connections.TryGetValue(subscriptionName, out var connection) == false)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return null;
            }

            return new SubscriptionConnectionsDetails()
            {
                Results = new()
                {
                    new SubscriptionConnectionDetails
                    {
                        ClientUri = connection?.ClientUri,
                        Strategy = connection?.Strategy
                    }
                },
                SubscriptionMode = "Single"
            };
        }
    }
}
