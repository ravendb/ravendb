using System.Net;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Builder;
using Raven.Server.Documents.Handlers.Processors.Subscriptions;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Subscriptions
{
    internal class ShardedSubscriptionsHandlerProcessorForGetConnectionDetails : AbstractSubscriptionsHandlerProcessorForGetConnectionDetails<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedSubscriptionsHandlerProcessorForGetConnectionDetails([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override SubscriptionConnectionsDetails GetConnectionDetails(TransactionOperationContext context, string subscriptionName)
        {
            var state = RequestHandler.DatabaseContext.Subscriptions.GetSubscriptionConnectionsState(context, subscriptionName);
            if (state == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return null;
            }

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, " make sharded & non-sharded identical.");
            return state.GetSubscriptionConnectionsDetails();
        }
    }
}
