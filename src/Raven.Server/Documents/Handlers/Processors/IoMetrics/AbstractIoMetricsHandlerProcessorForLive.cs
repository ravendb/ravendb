using JetBrains.Annotations;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.IoMetrics;

internal abstract class AbstractIoMetricsHandlerProcessorForLive<TRequestHandler, TOperationContext> : AbstractHandlerWebSocketProxyProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractIoMetricsHandlerProcessorForLive([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override string GetRemoteEndpointUrl(string databaseName) => $"/databases/{databaseName}/debug/io-metrics/live";
}
