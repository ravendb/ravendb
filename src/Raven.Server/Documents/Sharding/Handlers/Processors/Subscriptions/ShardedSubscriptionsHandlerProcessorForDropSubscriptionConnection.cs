using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Documents.Handlers.Processors.Subscriptions;
using Raven.Server.Documents.Sharding.Operations;
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
                if (string.IsNullOrEmpty(subscriptionName))
                {
                    if (subscriptionId.HasValue == false)
                        throw new ArgumentException("Subscription drop operation must get either subscription name or subscription task id");
                    
                    //get subscription name from state
                    using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    {
                        subscriptionName = ServerStore.Cluster.Subscriptions.GetSubscriptionNameById(context, RequestHandler.DatabaseName, subscriptionId.Value);
                    }

                    if (subscriptionName == null)
                        throw new SubscriptionDoesNotExistException($"Could not find a subscription with a subscription id of {subscriptionId.Value}");
                }

                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "Make this identical to the normal EP");

                if (RequestHandler.DatabaseContext.Subscriptions.SubscriptionsConnectionsState.TryRemove(subscriptionId.Value, out var state))
                {
                    state.Dispose();
                }
            }
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "Handle status code");
        }
    }
}
