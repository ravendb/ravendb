using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Raven.Client.Documents.Replication;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Replication;
using Raven.Server.Documents.Replication.Stats;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractReplicationHandlerProcessorForGetActiveConnections<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<ReplicationActiveConnectionsPreview, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractReplicationHandlerProcessorForGetActiveConnections([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override RavenCommand<ReplicationActiveConnectionsPreview> CreateCommandForNode(string nodeTag) => new GetReplicationActiveConnectionsInfoCommand(nodeTag);
    }

    public class ReplicationActiveConnectionsPreview
    {
        public List<IncomingConnectionInfo> IncomingConnections;

        public List<ReplicationNode> OutgoingConnections;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(IncomingConnections)] = new DynamicJsonArray(IncomingConnections.Select(i => i.ToJson())),
                [nameof(OutgoingConnections)] = new DynamicJsonArray(OutgoingConnections.Select(o => o.ToJson()))
            };
        }
    }
}
