using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using JetBrains.Annotations;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Handlers.Processors.Subscriptions;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Subscriptions
{
    internal class ShardedSubscriptionsHandlerProcessorForGetSubscription : AbstractSubscriptionsHandlerProcessorForGetSubscription<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedSubscriptionsHandlerProcessorForGetSubscription([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override IEnumerable<SubscriptionState> GetSubscriptions(TransactionOperationContext context, int start, int pageSize, bool history, bool running, long? id, string name)
        {
            if (history)
                throw new ArgumentException(nameof(history) + " not supported");

            var subscriptions = new List<SubscriptionState>();
            if (string.IsNullOrEmpty(name) && id == null)
            {
                var allSubs = SubscriptionsClusterStorage.GetAllSubscriptionsWithoutState(context, RequestHandler.DatabaseName, start, pageSize);
                if (allSubs == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return null;
                }

                if (running)
                {
                    foreach (var sub in allSubs)
                    {
                        if (ShardedSubscriptionConnection.Connections.ContainsKey(sub.SubscriptionName))
                        {
                            subscriptions.Add(sub);
                        }
                    }
                }
                else
                {
                    subscriptions = allSubs.ToList();
                }
            }
            else
            {
                var subscription = id == null
                    ? ServerStore.Cluster.Subscriptions.ReadSubscriptionStateByName(context, RequestHandler.DatabaseName, name)
                    : ServerStore.Cluster.Subscriptions.ReadSubscriptionStateById(context, RequestHandler.DatabaseName, id.Value);

                if (subscription == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return null;
                }

                if (running)
                {
                    if (ShardedSubscriptionConnection.Connections.ContainsKey(subscription.SubscriptionName) == false)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return null;
                    }
                }

                subscriptions = new List<SubscriptionState> { subscription };
            }

            return subscriptions;
        }
    }
}
