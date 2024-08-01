using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using static Raven.Server.Documents.Subscriptions.SubscriptionStorage;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal abstract class AbstractSubscriptionsHandlerProcessorForGetSubscription<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractSubscriptionsHandlerProcessorForGetSubscription([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract IEnumerable<SubscriptionState> GetSubscriptions(ClusterOperationContext context, int start, int pageSize, bool history, bool running, long? id, string name);

        public override async ValueTask ExecuteAsync()
        {
            var start = RequestHandler.GetStart();
            var pageSize = RequestHandler.GetPageSize();
            var history = RequestHandler.GetBoolValueQueryString("history", required: false) ?? false;
            var running = RequestHandler.GetBoolValueQueryString("running", required: false) ?? false;
            var id = RequestHandler.GetLongQueryString("id", required: false);
            var name = RequestHandler.GetStringQueryString("name", required: false);

            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                var subscriptions = GetSubscriptions(context, start, pageSize, history, running, id, name);
                if(subscriptions == null)
                    return;

                await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, RequestHandler.ResponseBodyStream()))
                {
                    WriteGetAllResult(writer, subscriptions, context);
                }
            }
        }

        internal void WriteGetAllResult(AsyncBlittableJsonTextWriterForDebug writer, IEnumerable<SubscriptionState> subscriptions, ClusterOperationContext context)
        {
            writer.WriteStartObject();
            writer.WriteArray(context, "Results", subscriptions.Select(SubscriptionStateAsJson), (w, c, subscription) => c.Write(w, subscription));
            writer.WriteEndObject();
        }

        protected virtual DynamicJsonValue SubscriptionStateAsJson(SubscriptionState state)
        {
            var json = new DynamicJsonValue
            {
                [nameof(SubscriptionState.SubscriptionId)] = state.SubscriptionId,
                [nameof(SubscriptionState.SubscriptionName)] = state.SubscriptionName,
                [nameof(SubscriptionState.ChangeVectorForNextBatchStartingPoint)] = state.ChangeVectorForNextBatchStartingPoint,
                [nameof(SubscriptionState.ShardingState)] = state.ShardingState?.ToJson(),
                [nameof(SubscriptionState.Query)] = state.Query,
                [nameof(SubscriptionState.Disabled)] = state.Disabled,
                [nameof(SubscriptionState.LastClientConnectionTime)] = state.LastClientConnectionTime,
                [nameof(SubscriptionState.LastBatchAckTime)] = state.LastBatchAckTime,
                [nameof(SubscriptionState.MentorNode)] = state.MentorNode,
                [nameof(SubscriptionState.PinToMentorNode)] = state.PinToMentorNode,
                [nameof(SubscriptionState.ArchivedDataProcessingBehavior)] = state.ArchivedDataProcessingBehavior
            };

            if (state is SubscriptionGeneralDataAndStats stateAndStats)
            {
                json[nameof(SubscriptionGeneralDataAndStats.Connections)] = GetSubscriptionConnectionsJson(stateAndStats.Connections);
                json[nameof(SubscriptionGeneralDataAndStats.RecentConnections)] = stateAndStats.RecentConnections == null
                    ? Array.Empty<SubscriptionConnectionInfo>()
                    : stateAndStats.RecentConnections.Select(r => r.ToJson());
                json[nameof(SubscriptionGeneralDataAndStats.RecentRejectedConnections)] = stateAndStats.RecentRejectedConnections == null
                    ? Array.Empty<SubscriptionConnectionInfo>()
                    : stateAndStats.RecentRejectedConnections.Select(r => r.ToJson());
                json[nameof(SubscriptionGeneralDataAndStats.CurrentPendingConnections)] = stateAndStats.CurrentPendingConnections == null
                    ? Array.Empty<SubscriptionConnectionInfo>()
                    : stateAndStats.CurrentPendingConnections.Select(r => r.ToJson());
            }

            return json;
        }

        private static DynamicJsonArray GetSubscriptionConnectionsJson(List<SubscriptionConnection> subscriptionList)
        {
            if (subscriptionList == null)
                return new DynamicJsonArray();

            return new DynamicJsonArray(subscriptionList.Select(GetSubscriptionConnectionJson));
        }

        protected static DynamicJsonValue GetSubscriptionConnectionJson<T>(SubscriptionConnectionBase<T> x) where T : AbstractIncludesCommand
        {
            if (x == null)
                return new DynamicJsonValue();

            return new DynamicJsonValue()
            {
                [nameof(SubscriptionConnection.ClientUri)] = x.ClientUri,
                [nameof(SubscriptionConnection.Strategy)] = x.Strategy,
                [nameof(SubscriptionConnection.Stats)] = GetConnectionStatsJson(x.Stats),
                [nameof(SubscriptionConnection.ConnectionException)] = x.ConnectionException?.Message,
                ["TcpConnectionStats"] = x.TcpConnection.GetConnectionStats(),
                [nameof(SubscriptionConnection.RecentSubscriptionStatuses)] = new DynamicJsonArray(x.RecentSubscriptionStatuses?.ToArray() ?? Array.Empty<string>())
            };
        }

        private static DynamicJsonValue GetConnectionStatsJson(SubscriptionStatsCollector x)
        {
            return new DynamicJsonValue()
            {
                [nameof(SubscriptionStatsCollector.Metrics.AckRate)] = x.Metrics.AckRate?.CreateMeterData(),
                [nameof(SubscriptionStatsCollector.Metrics.BytesRate)] = x.Metrics.BytesRate?.CreateMeterData(),
                [nameof(SubscriptionStatsCollector.Metrics.ConnectedAt)] = x.Metrics.ConnectedAt,
                [nameof(SubscriptionStatsCollector.Metrics.DocsRate)] = x.Metrics.DocsRate?.CreateMeterData(),
                [nameof(SubscriptionStatsCollector.Metrics.LastAckReceivedAt)] = x.Metrics.LastAckReceivedAt,
                [nameof(SubscriptionStatsCollector.Metrics.LastMessageSentAt)] = x.Metrics.LastMessageSentAt,
            };
        }
    }
}
