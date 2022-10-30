using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Utils.Metrics.Commands;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Stats
{
    internal abstract class AbstractStatsHandlerProcessorForGetMetricsBytes<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<object, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractStatsHandlerProcessorForGetMetricsBytes([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override RavenCommand CreateCommandForNode(string nodeTag)
        {
            var empty = RequestHandler.GetBoolValueQueryString("empty", required: false) ?? true;
            return new GetDatabaseMetricsCommand(nodeTag, bytesOnly: true, filterEmpty: empty);
        }
    }
}
