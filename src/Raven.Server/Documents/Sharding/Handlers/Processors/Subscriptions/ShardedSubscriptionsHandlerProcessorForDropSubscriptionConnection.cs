using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
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
            if (subscriptionId.HasValue == false && string.IsNullOrEmpty(subscriptionName))
                throw new ArgumentException("Subscription drop operation must get either subscription name or subscription task id");

            if (subscriptionId.HasValue == false)
            {
                using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (context.OpenReadTransaction())
                {
                    subscriptionId = RequestHandler.DatabaseContext.SubscriptionsStorage.GetSubscriptionByName(context, subscriptionName)
                        .SubscriptionId;
                }
            }

            if (string.IsNullOrEmpty(subscriptionName))
            {
                using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (context.OpenReadTransaction())
                {
                    subscriptionName = RequestHandler.DatabaseContext.SubscriptionsStorage.GetSubscriptionById(context, subscriptionId.Value).SubscriptionName;
                }
            }

            var op = new ShardedDropSubscriptionConnectionOperation(HttpContext, subscriptionName);
            await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "RavenDB-19079 Make this identical to the normal EP");
            if (RequestHandler.DatabaseContext.SubscriptionsStorage.Subscriptions.TryRemove(subscriptionId.Value, out var state))
            {
                state.Dispose();
            }

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "RavenDB-19079 Handle status code");
        }
    }
}
