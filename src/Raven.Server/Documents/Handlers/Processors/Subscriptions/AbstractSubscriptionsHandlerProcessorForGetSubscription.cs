using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
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

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal abstract class AbstractSubscriptionsHandlerProcessorForGetSubscription<TRequestHandler, TOperationContext, TState> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TState : SubscriptionState
    {
        protected AbstractSubscriptionsHandlerProcessorForGetSubscription([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract IEnumerable<TState> GetAllSubscriptions(ClusterOperationContext context, int start, int pageSize, bool history, bool running);
        protected abstract TState GetSubscriptionByName(ClusterOperationContext context, bool history, bool running, string name);
        protected abstract TState GetSubscriptionById(ClusterOperationContext context, bool history, bool running, long id);

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

        private IEnumerable<TState> GetSubscriptions(ClusterOperationContext context, int start, int pageSize, bool history, bool running, long? id, string name)
        {
            IEnumerable<TState> subscriptions;
            if (string.IsNullOrEmpty(name) && id == null)
            {
                subscriptions = GetAllSubscriptions(context, start, pageSize, history, running);
            }
            else
            {
                TState subscription;
                if (string.IsNullOrEmpty(name) == false)
                {
                    subscription = GetSubscriptionByName(context, history, running, name);
                }
                else if (id.HasValue)
                {
                    subscription = GetSubscriptionById(context, history, running, id.Value);
                }
                else
                {
                    throw new ArgumentNullException("Must receive either subscription id or subscription name in order to provide subscription data");
                }
                if (subscription == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return null;
                }

                subscriptions = new[] { subscription };
            }

            return subscriptions;
        }

        internal void WriteGetAllResult(AsyncBlittableJsonTextWriterForDebug writer, IEnumerable<TState> subscriptions, ClusterOperationContext context)
        {
            writer.WriteStartObject();
            writer.WriteArray(context, "Results", subscriptions.Select(SubscriptionStateAsJson), (w, c, subscription) => c.Write(w, subscription));
            writer.WriteEndObject();
        }

        protected virtual DynamicJsonValue SubscriptionStateAsJson(TState state)
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

            return json;
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
