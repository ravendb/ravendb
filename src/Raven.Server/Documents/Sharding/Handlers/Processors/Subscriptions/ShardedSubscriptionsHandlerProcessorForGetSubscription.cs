using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Subscriptions;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using static Raven.Server.Documents.Sharding.ShardedDatabaseContext.ShardedSubscriptionsStorage;
using static Raven.Server.Documents.Subscriptions.SubscriptionStorage;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Subscriptions
{
    internal sealed class ShardedSubscriptionsHandlerProcessorForGetSubscription : AbstractSubscriptionsHandlerProcessorForGetSubscription<ShardedDatabaseRequestHandler, TransactionOperationContext, ShardedSubscriptionData>
    {
        public ShardedSubscriptionsHandlerProcessorForGetSubscription([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override IEnumerable<ShardedSubscriptionData> GetAllSubscriptions(ClusterOperationContext context, int start, int pageSize, bool history, bool running)
        {
            return running
                ? RequestHandler.DatabaseContext.SubscriptionsStorage.GetAllRunningSubscriptions(context, history, start, pageSize)
                : RequestHandler.DatabaseContext.SubscriptionsStorage.GetAllSubscriptions(context, history, start, pageSize);
        }

        protected override ShardedSubscriptionData GetSubscriptionByName(ClusterOperationContext context, bool history, bool running, string name)
        {
            return RequestHandler.DatabaseContext.SubscriptionsStorage.GetSubscriptionWithDataByNameFromServerStore(context, name, history, running);
        }

        protected override ShardedSubscriptionData GetSubscriptionById(ClusterOperationContext context, bool history, bool running, long id)
        {
            return RequestHandler.DatabaseContext.SubscriptionsStorage.GetSubscriptionWithDataByIdFromServerStore(context, id, history, running);
            ;
        }

        protected override DynamicJsonValue SubscriptionStateAsJson(ShardedSubscriptionData state)
        {
            var json = base.SubscriptionStateAsJson(state);
            json[nameof(SubscriptionGeneralDataAndStats.Connections)] = GetSubscriptionConnectionsJson(state.Connections);
            json[nameof(SubscriptionGeneralDataAndStats.RecentConnections)] = state.RecentConnections == null
                ? Array.Empty<SubscriptionConnectionInfo>()
                : state.RecentConnections.Select(r => r.ToJson());
            json[nameof(SubscriptionGeneralDataAndStats.RecentRejectedConnections)] = state.RecentRejectedConnections == null
                ? Array.Empty<SubscriptionConnectionInfo>()
                : state.RecentRejectedConnections.Select(r => r.ToJson());
            json[nameof(SubscriptionGeneralDataAndStats.CurrentPendingConnections)] = state.CurrentPendingConnections == null
                ? Array.Empty<SubscriptionConnectionInfo>()
                : state.CurrentPendingConnections.Select(r => r.ToJson());
            json[nameof(ShardedSubscriptionData.RecentShardedWorkers)] = state.RecentShardedWorkers == null ? Array.Empty<ShardedSubscriptionWorkerInfo>()
                : state.RecentShardedWorkers.Select(x => x.ToJson());
            json[nameof(ShardedSubscriptionData.ShardedWorkers)] = GetShardedWorkersJson(state.ShardedWorkers);

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
