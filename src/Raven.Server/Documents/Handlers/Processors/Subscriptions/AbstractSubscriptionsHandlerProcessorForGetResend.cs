using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal abstract class AbstractSubscriptionsHandlerProcessorForGetResend<TRequestHandler, TOperationContext, TSubscriptionState> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TSubscriptionState : AbstractSubscriptionConnectionsState
    {
        protected readonly AbstractSubscriptionStorage<TSubscriptionState> SubscriptionStorage;

        protected AbstractSubscriptionsHandlerProcessorForGetResend([NotNull] TRequestHandler requestHandler, [NotNull] AbstractSubscriptionStorage<TSubscriptionState> subscriptionStorage) : base(requestHandler)
        {
            SubscriptionStorage = subscriptionStorage ?? throw new ArgumentNullException(nameof(subscriptionStorage));
        }

        protected abstract HashSet<long> GetActiveBatches(ClusterOperationContext context, SubscriptionState subscriptionState);

        public override async ValueTask ExecuteAsync()
        {
            var subscriptionName = RequestHandler.GetStringQueryString("name");

            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            {
                HashSet<long> activeBatches;
                IEnumerable<ResendItem> resendItems;
                using (context.OpenReadTransaction())
                {
                    SubscriptionState subscriptionState;
                    try
                    {
                        subscriptionState = SubscriptionStorage.GetSubscriptionByName(context, subscriptionName);
                    }
                    catch (SubscriptionDoesNotExistException)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return;
                    }
                    
                    resendItems = AbstractSubscriptionConnectionsState.GetResendItems(context, RequestHandler.DatabaseName, subscriptionState.SubscriptionId);
                    activeBatches = GetActiveBatches(context, subscriptionState);

                    if (activeBatches == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return;
                    }

                    await WriteSubscriptionBatchesStateAsync(context, resendItems, activeBatches);
                }
            }
        }

        protected async Task WriteSubscriptionBatchesStateAsync(JsonOperationContext context, IEnumerable<ResendItem> resendItems, HashSet<long> activeBatches)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WriteArray(nameof(SubscriptionBatchesState.Active), activeBatches);
                writer.WriteComma();
                writer.WriteArray(nameof(SubscriptionBatchesState.Results), resendItems?.Select(r => r.ToJson()), context);
                writer.WriteEndObject();
            }
        }
    }

    public sealed class SubscriptionBatchesState
    {
        public HashSet<long> Active;
        public List<ResendItem> Results;
    }
}
