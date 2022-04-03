using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Json.Serialization;
using Raven.Server.TrafficWatch;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Counters
{
    internal abstract class AbstractCountersHandlerProcessorForPostCounters<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        public AbstractCountersHandlerProcessorForPostCounters([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        {
        }

        protected abstract ValueTask<CountersDetail> ApplyCountersOperationsAsync(TOperationContext context, CounterBatch counterBatch);

        public override async ValueTask ExecuteAsync()
        {
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                var countersBlittable = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "counters");

                var counterBatch = JsonDeserializationClient.CounterBatch(countersBlittable);

                if (TrafficWatchManager.HasRegisteredClients)
                    RequestHandler.AddStringToHttpContext(countersBlittable.ToString(), TrafficWatchChangeType.Counters);

                var result = await ApplyCountersOperationsAsync(context, counterBatch);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, result.ToJson());
                }
            }
        }
    }
}
