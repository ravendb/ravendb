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
        protected AbstractReplicationHandlerProcessorForProgress([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override RavenCommand<ReplicationTaskProgress[]> CreateCommandForNode(string nodeTag)
        {
            var names = GetNames();

            return new GetReplicationOngoingTasksProgressCommand(names, nodeTag);
        }

        protected StringValues GetNames() => RequestHandler.GetStringValuesQueryString("name", required: false);
    }
}
