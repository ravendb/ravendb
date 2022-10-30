using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Utils.Metrics.Commands;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Stats
{
    internal abstract class AbstractStatsHandlerProcessorForGetMetrics<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<object, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractStatsHandlerProcessorForGetMetrics([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override RavenCommand CreateCommandForNode(string nodeTag) => new GetDatabaseMetricsCommand(nodeTag);
    }
}
