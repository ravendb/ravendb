using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal abstract class AbstractSubscriptionsHandlerProcessorForGetSubscription<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        public AbstractSubscriptionsHandlerProcessorForGetSubscription([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract IEnumerable<SubscriptionState> GetSubscriptions(TransactionOperationContext context, int start, int pageSize, bool history, bool running, long? id, string name);

        public override async ValueTask ExecuteAsync()
        {
            var start = RequestHandler.GetStart();
            var pageSize = RequestHandler.GetPageSize();
            var history = RequestHandler.GetBoolValueQueryString("history", required: false) ?? false;
            var running = RequestHandler.GetBoolValueQueryString("running", required: false) ?? false;
            var id = RequestHandler.GetLongQueryString("id", required: false);
            var name = RequestHandler.GetStringQueryString("name", required: false);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var subscriptions = GetSubscriptions(context, start, pageSize, history, running, id, name);
                if(subscriptions == null)
                    return;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    WriteGetAllResult(writer, subscriptions, context);
                }
            }
        }

        internal static void WriteGetAllResult(AsyncBlittableJsonTextWriter writer, IEnumerable<SubscriptionState> subscriptions, TransactionOperationContext context)
        {
            writer.WriteStartObject();
            writer.WriteArray(context, "Results", subscriptions.Select(SubscriptionStateAsJson), (w, c, subscription) => c.Write(w, subscription));
            writer.WriteEndObject();
        }

        private static DynamicJsonValue SubscriptionStateAsJson(SubscriptionState state)
        {
            var json = new DynamicJsonValue
            {
                [nameof(SubscriptionState.SubscriptionId)] = state.SubscriptionId,
                [nameof(SubscriptionState.SubscriptionName)] = state.SubscriptionName,
                [nameof(SubscriptionState.ChangeVectorForNextBatchStartingPoint)] = state.ChangeVectorForNextBatchStartingPoint,
                [nameof(SubscriptionState.ChangeVectorForNextBatchStartingPointPerShard)] = state.ChangeVectorForNextBatchStartingPointPerShard?.ToJson(),
                [nameof(SubscriptionState.Query)] = state.Query,
                [nameof(SubscriptionState.Disabled)] = state.Disabled,
                [nameof(SubscriptionState.LastClientConnectionTime)] = state.LastClientConnectionTime,
                [nameof(SubscriptionState.LastBatchAckTime)] = state.LastBatchAckTime
            };

            if (state is SubscriptionStorage.SubscriptionGeneralDataAndStats stateAndStats)
            {
                json["Connection"] = GetSubscriptionConnectionJson(stateAndStats.Connection);
                json["Connections"] = GetSubscriptionConnectionsJson(stateAndStats.Connections);
                json["RecentConnections"] = stateAndStats.RecentConnections?.Select(r => new DynamicJsonValue()
                {
                    ["State"] = new DynamicJsonValue()
                    {
                        ["LatestChangeVectorClientACKnowledged"] = r.SubscriptionState.ChangeVectorForNextBatchStartingPoint,
                        ["LatestChangeVectorsClientACKnowledged"] = r.SubscriptionState.ChangeVectorForNextBatchStartingPointPerShard?.ToJson(),
                        ["Query"] = r.SubscriptionState.Query
                    },
                    ["Connection"] = GetSubscriptionConnectionJson(r)
                });
                json["FailedConnections"] = stateAndStats.RecentRejectedConnections?.Select(r => new DynamicJsonValue()
                {
                    ["State"] = new DynamicJsonValue()
                    {
                        ["LatestChangeVectorClientACKnowledged"] = r.SubscriptionState.ChangeVectorForNextBatchStartingPoint,
                        ["LatestChangeVectorsClientACKnowledged"] = r.SubscriptionState.ChangeVectorForNextBatchStartingPointPerShard?.ToJson(),
                        ["Query"] = r.SubscriptionState.Query
                    },
                    ["Connection"] = GetSubscriptionConnectionJson(r)
                });
            }

            return json;
        }

        private static DynamicJsonArray GetSubscriptionConnectionsJson(List<SubscriptionConnection> subscriptionList)
        {
            if (subscriptionList == null)
                return new DynamicJsonArray();

            return new DynamicJsonArray(subscriptionList.Select(s => GetSubscriptionConnectionJson(s)));
        }

        private static DynamicJsonValue GetSubscriptionConnectionJson(SubscriptionConnection x)
        {
            if (x == null)
                return new DynamicJsonValue();

            return new DynamicJsonValue()
            {
                [nameof(SubscriptionConnection.ClientUri)] = x.ClientUri,
                [nameof(SubscriptionConnection.Strategy)] = x.Strategy,
                [nameof(SubscriptionConnection.StatsCollector)] = GetConnectionStatsJson(x.StatsCollector),
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
