using System.Threading.Tasks;
using JetBrains.Annotations;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Stats
{
    internal abstract class AbstractStatsHandlerProcessorForGetMetrics<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractStatsHandlerProcessorForGetMetrics([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask<DynamicJsonValue> GetDatabaseMetricsAsync(JsonOperationContext context);

        public override async ValueTask ExecuteAsync()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                var metrics = await GetDatabaseMetricsAsync(context);
                context.Write(writer, metrics);
            }
        }
    }
}
