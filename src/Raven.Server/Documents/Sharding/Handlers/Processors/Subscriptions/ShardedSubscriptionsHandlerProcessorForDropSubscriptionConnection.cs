﻿using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Subscriptions;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Subscriptions
{
    internal class ShardedSubscriptionsHandlerProcessorForDropSubscriptionConnection : AbstractSubscriptionsHandlerProcessorForDropSubscriptionConnection<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedSubscriptionsHandlerProcessorForDropSubscriptionConnection([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask DropSubscriptionAsync(long? subscriptionId, string subscriptionName, string workerId)
        {
            try
            {
                var op = new ShardedDropSubscriptionConnectionOperation(HttpContext, subscriptionName, subscriptionId, workerId);
                await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);
            }
            finally
            {
                if (ShardedSubscriptionConnection.Connections.TryRemove(subscriptionName, out ShardedSubscriptionConnection connection))
                {
                    connection.Dispose();
                }
            }
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "Handle status code");
        }
    }
}
