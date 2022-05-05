using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForPerformanceLive<TRequestHandler, TOperationContext> : AbstractHandlerWebSocketProxyProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractIndexHandlerProcessorForPerformanceLive([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override string GetRemoteEndpointUrl(string databaseName) => $"/databases/{databaseName}/indexes/performance/live";

    protected bool GetIncludeSideBySide() => RequestHandler.GetBoolValueQueryString("includeSideBySide", false) ?? false;

    protected StringValues GetNames() => RequestHandler.GetStringValuesQueryString("name", required: false);
}
