using JetBrains.Annotations;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions;

internal abstract class AbstractSubscriptionsHandlerProcessorForPerformanceLive<TRequestHandler, TOperationContext> : AbstractHandlerWebSocketProxyProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractSubscriptionsHandlerProcessorForPerformanceLive([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override string GetRemoteEndpointUrl(string databaseName) => $"/databases/{databaseName}/etl/performance/live";
}
