using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Json;
using Raven.Server.TrafficWatch;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal abstract class AbstractSubscriptionsHandlerProcessorForPutSubscription<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        public AbstractSubscriptionsHandlerProcessorForPutSubscription([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask CreateInternalAsync(BlittableJsonReaderObject bjro, SubscriptionCreationOptions options, TOperationContext context, long? id,
            bool? disabled);

        public override async ValueTask ExecuteAsync()
        {
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), null);
                var options = JsonDeserializationServer.SubscriptionCreationParams(json);

                if (TrafficWatchManager.HasRegisteredClients)
                    RequestHandler.AddStringToHttpContext(json.ToString(), TrafficWatchChangeType.Subscriptions);

                var id = RequestHandler.GetLongQueryString("id", required: false);
                var disabled = options.Disabled;

                await CreateInternalAsync(json, options, context, id, disabled);
            }
        }
    }
}
