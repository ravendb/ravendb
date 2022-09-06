using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Documents.Replication;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Replication;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Json;
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

        public List<OutgoingConnectionInfo> OutgoingConnections;

        public class OutgoingConnectionInfo
        {
            public string Url;

            public string Database;

            public bool Disabled;

            public static DynamicJsonValue ToJson(ReplicationNode replicationNode)
            {
                return new DynamicJsonValue
                {
                    [nameof(Url)] = replicationNode.Url,
                    [nameof(Database)] = replicationNode.Database,
                    [nameof(Disabled)] = replicationNode.Disabled
                };
            }

            public static OutgoingConnectionInfo FromJson(BlittableJsonReaderObject json)
            {
                if (json == null)
                    return null;

                return JsonDeserializationServer.ReplicationOutgoingConnectionInfo(json);
            }
        }
    }
}
