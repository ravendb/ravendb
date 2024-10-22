using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Replication;
using Raven.Server.Documents.Replication.Stats;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractReplicationHandlerProcessorForProgress<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<ReplicationTaskProgress[], TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        private readonly bool _internalReplication;

        protected AbstractReplicationHandlerProcessorForProgress([NotNull] TRequestHandler requestHandler, bool internalReplication) : base(requestHandler)
        {
            _internalReplication = internalReplication;
        }

        protected override RavenCommand<ReplicationTaskProgress[]> CreateCommandForNode(string nodeTag)
        {
            if (_internalReplication)
                return new GetOutgoingInternalReplicationProgressCommand(nodeTag);

            var names = GetNames();
            return new GetReplicationOngoingTasksProgressCommand(names, nodeTag);
        }

        protected StringValues GetNames() => RequestHandler.GetStringValuesQueryString("name", required: false);
    }
}
