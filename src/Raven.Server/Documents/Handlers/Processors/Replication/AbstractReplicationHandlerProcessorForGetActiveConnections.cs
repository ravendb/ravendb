using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Replication;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Replication;
using Raven.Server.Documents.Replication;
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

    public sealed class ReplicationActiveConnectionsPreview : IFillFromBlittableJson
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

        public void FillFromBlittableJson(BlittableJsonReaderObject json)
        {
            FillIncomingConnections(json);
            FillOutgoingConnections(json);
        }

        private void FillIncomingConnections(BlittableJsonReaderObject json)
        {
            IncomingConnections = new List<IncomingConnectionInfo>();
            if (json.TryGetMember(nameof(IncomingConnections), out var result) && result is BlittableJsonReaderArray bjra)
            {
                foreach (BlittableJsonReaderObject bjro in bjra)
                {
                    var incomingConnection = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<IncomingConnectionInfo>(bjro, "IncomingConnectionInfo");
                    IncomingConnections.Add(incomingConnection);
                }
            }
        }

        private void FillOutgoingConnections(BlittableJsonReaderObject json)
        {
            OutgoingConnections = new List<ReplicationNode>();
            if (json.TryGetMember(nameof(OutgoingConnections), out var result) && result is BlittableJsonReaderArray bjra)
            {
                foreach (BlittableJsonReaderObject bjro in bjra)
                {
                    var outgoingConnection = ReplicationHelper.GetReplicationNodeByType(bjro);
                    OutgoingConnections.Add(outgoingConnection);
                }
            }
        }
    }
}
