using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using static Raven.Server.Documents.Subscriptions.SubscriptionStorage;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal sealed class SubscriptionsHandlerProcessorForGetSubscription 
        : AbstractSubscriptionsHandlerProcessorForGetSubscription<DatabaseRequestHandler, DocumentsOperationContext, SubscriptionGeneralDataAndStats>
    {
        public SubscriptionsHandlerProcessorForGetSubscription([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override IEnumerable<SubscriptionGeneralDataAndStats> GetAllSubscriptions(ClusterOperationContext context, int start, int pageSize, bool history, bool running)
        {
           return running
               ? RequestHandler.Database.SubscriptionStorage.GetAllRunningSubscriptions(context, history, start, pageSize)
               : RequestHandler.Database.SubscriptionStorage.GetAllSubscriptions(context, history, start, pageSize);
        }

        protected override SubscriptionGeneralDataAndStats GetSubscriptionByName(ClusterOperationContext context, bool history, bool running, string name)
        {
            return RequestHandler.Database.SubscriptionStorage.GetSubscriptionWithDataByNameFromServerStore(context, name, history, running);
        }

        protected override SubscriptionGeneralDataAndStats GetSubscriptionById(ClusterOperationContext context, bool history, bool running, long id)
        {
           return RequestHandler.Database.SubscriptionStorage.GetSubscriptionWithDataByIdFromServerStore(context, id, history, running);
        }

        protected override DynamicJsonValue SubscriptionStateAsJson(SubscriptionGeneralDataAndStats state)
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

            return json;
        }

        private static DynamicJsonArray GetSubscriptionConnectionsJson(List<SubscriptionConnection> subscriptionList)
        {
            if (subscriptionList == null)
                return new DynamicJsonArray();

            return new DynamicJsonArray(subscriptionList.Select(GetSubscriptionConnectionJson));
        }
    }
}
