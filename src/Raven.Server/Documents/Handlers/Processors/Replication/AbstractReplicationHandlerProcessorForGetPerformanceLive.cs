using JetBrains.Annotations;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractReplicationHandlerProcessorForGetPerformanceLive<TRequestHandler, TOperationContext> : AbstractHandlerWebSocketProxyProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractReplicationHandlerProcessorForGetPerformanceLive([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override string GetRemoteEndpointUrl(string databaseName)
        {
            var url = $"/databases/{databaseName}/replication/performance/live";
            return url;
        }
    }
}
