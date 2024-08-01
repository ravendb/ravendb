using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using JetBrains.Annotations;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Handlers.Processors.Subscriptions;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using static Raven.Server.Documents.Sharding.ShardedDatabaseContext.ShardedSubscriptionsStorage;
using static Raven.Server.Documents.Subscriptions.SubscriptionStorage;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Subscriptions
{
    internal sealed class ShardedSubscriptionsHandlerProcessorForGetSubscription : AbstractSubscriptionsHandlerProcessorForGetSubscription<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedSubscriptionsHandlerProcessorForGetSubscription([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override IEnumerable<ShardedSubscriptionData> GetSubscriptions(ClusterOperationContext context, int start, int pageSize, bool history, bool running, long? id, string name)
        {
            IEnumerable<ShardedSubscriptionData> subscriptions;
            if (string.IsNullOrEmpty(name) && id == null)
            {
                subscriptions = running
                    ? RequestHandler.DatabaseContext.SubscriptionsStorage.GetAllRunningSubscriptions(context, history, start, pageSize)
                    : RequestHandler.DatabaseContext.SubscriptionsStorage.GetAllSubscriptions(context, history, start, pageSize);
            }
            else
            {
                var subscription = running
                    ? RequestHandler.DatabaseContext.SubscriptionsStorage
                        .GetRunningSubscription(context, id, name, history)
                    : RequestHandler.DatabaseContext.SubscriptionsStorage
                        .GetSubscription(context, id, name, history);

                if (subscription == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return null;
                }

                subscriptions = new[] { subscription };
            }

            return subscriptions;
        }

        protected override DynamicJsonValue SubscriptionStateAsJson(SubscriptionState state)
        {
            var json = base.SubscriptionStateAsJson(state);

            if (state is ShardedSubscriptionData shardedSubscriptionData)
            {
                json[nameof(SubscriptionGeneralDataAndStats.Connections)] = GetSubscriptionConnectionsJson(shardedSubscriptionData.Connections);
                json[nameof(SubscriptionGeneralDataAndStats.RecentConnections)] = shardedSubscriptionData.RecentConnections == null
                    ? Array.Empty<SubscriptionConnectionInfo>()
                    : shardedSubscriptionData.RecentConnections.Select(r => r.ToJson());
                json[nameof(SubscriptionGeneralDataAndStats.RecentRejectedConnections)] = shardedSubscriptionData.RecentRejectedConnections == null
                    ? Array.Empty<SubscriptionConnectionInfo>()
                    : shardedSubscriptionData.RecentRejectedConnections.Select(r => r.ToJson());
                json[nameof(SubscriptionGeneralDataAndStats.CurrentPendingConnections)] = shardedSubscriptionData.CurrentPendingConnections == null
                    ? Array.Empty<SubscriptionConnectionInfo>()
                    : shardedSubscriptionData.CurrentPendingConnections.Select(r => r.ToJson());
                json[nameof(ShardedSubscriptionData.RecentShardedWorkers)] = shardedSubscriptionData.RecentShardedWorkers == null ? Array.Empty<ShardedSubscriptionWorkerInfo>() 
                    : shardedSubscriptionData.RecentShardedWorkers.Select(x=>x.ToJson());
                json[nameof(ShardedSubscriptionData.ShardedWorkers)] = GetShardedWorkersJson(shardedSubscriptionData.ShardedWorkers);
            }

            return json;
        }

        private static DynamicJsonArray GetSubscriptionConnectionsJson(List<OrchestratedSubscriptionConnection> subscriptionList)
        {
            if (subscriptionList == null)
                return new DynamicJsonArray();

            return new DynamicJsonArray(subscriptionList.Select(GetSubscriptionConnectionJson));
        }

        private static DynamicJsonArray GetShardedWorkersJson(IDictionary<string, ShardedSubscriptionWorker> subscriptionList)
        {
            if (subscriptionList == null)
                return new DynamicJsonArray();

            return new DynamicJsonArray(subscriptionList.Select(w => ShardedSubscriptionWorkerInfo.Create(w.Key, w.Value).ToJson()));
        }
    }
}
