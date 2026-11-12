using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Replication;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Stats;
using Sparrow.Json;

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

    [UsedImplicitly(Reason = "Instantiated by the generated Blittable JSON deserializer.")]
    public sealed class ReplicationActiveConnectionsPreview : IFillFromBlittableJson
    {
        public List<ReplicationActiveConnectionInfo<IncomingConnectionInfo>> IncomingConnections;

        public List<ReplicationActiveConnectionInfo<ReplicationNode>> OutgoingConnections;

        public void FillFromBlittableJson(BlittableJsonReaderObject json)
        {
            FillIncomingConnections(json);
            FillOutgoingConnections(json);
        }

        private void FillIncomingConnections(BlittableJsonReaderObject json)
        {
            IncomingConnections = new List<ReplicationActiveConnectionInfo<IncomingConnectionInfo>>();
            if (json.TryGetMember(nameof(IncomingConnections), out var result) && result is BlittableJsonReaderArray bjra)
            {
                foreach (BlittableJsonReaderObject bjro in bjra)
                {
                    var incomingConnection = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<ReplicationActiveConnectionInfo<IncomingConnectionInfo>>(bjro, "IncomingConnection");
                    incomingConnection.Connection = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<IncomingConnectionInfo>(bjro, "IncomingConnectionInfo");
                    IncomingConnections.Add(incomingConnection);
                }
            }
        }

        private void FillOutgoingConnections(BlittableJsonReaderObject json)
        {
            OutgoingConnections = new List<ReplicationActiveConnectionInfo<ReplicationNode>>();
            if (json.TryGetMember(nameof(OutgoingConnections), out var result) && result is BlittableJsonReaderArray bjra)
            {
                foreach (BlittableJsonReaderObject bjro in bjra)
                {
                    var outgoingConnection = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<ReplicationActiveConnectionInfo<ReplicationNode>>(bjro, "OutgoingConnection");
                    outgoingConnection.Connection = ReplicationHelper.GetReplicationNodeByType(bjro);
                    OutgoingConnections.Add(outgoingConnection);
                }
            }
        }
    }

    // Read-only endpoint row: keep the original connection type without adding runtime fields to task configuration.
    public sealed class ReplicationActiveConnectionInfo<TConnection>
    {
        public TConnection Connection;
        public PullReplicationChangeVectorWireMode? ChangeVectorWireMode;
        public PullReplicationMode? PullReplicationMode;
        public string PullReplicationDefinitionName;
    }
}
