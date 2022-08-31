using JetBrains.Annotations;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractReplicationHandlerProcessorForGetPulsesLive<TRequestHandler, TOperationContext> : AbstractHandlerWebSocketProxyProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractReplicationHandlerProcessorForGetPulsesLive([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override string GetRemoteEndpointUrl(string databaseName)
        {
            var url = $"/databases/{databaseName}/replication/pulses/live";
            return url;
        }
    }
}
