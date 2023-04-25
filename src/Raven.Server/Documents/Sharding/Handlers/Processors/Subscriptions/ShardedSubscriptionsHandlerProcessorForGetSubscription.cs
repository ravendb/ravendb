using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using JetBrains.Annotations;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Handlers.Processors.Subscriptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Subscriptions
{
    internal class ShardedSubscriptionsHandlerProcessorForGetSubscription : AbstractSubscriptionsHandlerProcessorForGetSubscription<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedSubscriptionsHandlerProcessorForGetSubscription([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override IEnumerable<SubscriptionState> GetSubscriptions(ClusterOperationContext context, int start, int pageSize, bool history, bool running, long? id, string name)
        {
            if (history)
                throw new ArgumentException(nameof(history) + " not supported");

            var subscriptions = new List<SubscriptionState>();
            if (string.IsNullOrEmpty(name) && id == null)
            {
                var allSubs = RequestHandler.DatabaseContext.SubscriptionsStorage.GetAllSubscriptionsFromServerStore(context, start, pageSize);
                if (allSubs == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return null;
                }

                if (running)
                {
                    foreach (var sub in allSubs)
                    {
                        var state = RequestHandler.DatabaseContext.SubscriptionsStorage.GetSubscriptionConnectionsState(context, sub.SubscriptionName);
                        if (state != null)
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
                    ? RequestHandler.DatabaseContext.SubscriptionsStorage.GetSubscriptionByName(context, name)
                    : RequestHandler.DatabaseContext.SubscriptionsStorage.GetSubscriptionById(context, id.Value);

                if (subscription == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return null;
                }

                if (running)
                {
                    var state = RequestHandler.DatabaseContext.SubscriptionsStorage.GetSubscriptionConnectionsState(context, subscription.SubscriptionName);
                    if (state == null)
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
