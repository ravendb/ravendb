using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Replication;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractReplicationHandlerProcessorForGetIncomingRejectionInfo<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<object, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractReplicationHandlerProcessorForGetIncomingRejectionInfo([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override RavenCommand<object> CreateCommandForNode(string nodeTag)
        {
            return new GetIncomingReplicationRejectionInfoCommand(nodeTag);
        }
    }
}
