using System.Collections.Generic;
using System.Net.Http;
using JetBrains.Annotations;
using Raven.Client.Documents.Replication;
using Raven.Client.Http;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using static Raven.Server.Documents.Handlers.Processors.Replication.ReplicationActiveConnectionsPreview;

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

    internal class GetReplicationActiveConnectionsInfoCommand : RavenCommand<ReplicationActiveConnectionsPreview>
    {
        public GetReplicationActiveConnectionsInfoCommand()
        {
        }

        public GetReplicationActiveConnectionsInfoCommand(string nodeTag)
        {
            SelectedNodeTag = nodeTag;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/replication/active-connections";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };

            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            var incomingConnectionsInfo = new List<IncomingConnectionInfo>();
            if (response.TryGet(nameof(ReplicationActiveConnectionsPreview.IncomingConnections), out BlittableJsonReaderArray bjra))
            {
                foreach (BlittableJsonReaderObject bjro in bjra)
                {
                    var incomingConnectionInfo = IncomingConnectionInfo.FromJson(bjro);
                    incomingConnectionsInfo.Add(incomingConnectionInfo);
                }
            }

            var outgoingConnectionsInfo = new List<OutgoingConnectionInfo>();
            if (response.TryGet(nameof(ReplicationActiveConnectionsPreview.OutgoingConnections), out bjra))
            {
                foreach (BlittableJsonReaderObject bjro in bjra)
                {
                    var outgoingConnectionInfo = OutgoingConnectionInfo.FromJson(bjro);
                    outgoingConnectionsInfo.Add(outgoingConnectionInfo);
                }
            }
           
            Result = new ReplicationActiveConnectionsPreview { IncomingConnections = incomingConnectionsInfo, OutgoingConnections = outgoingConnectionsInfo };
        }

        public override bool IsReadRequest => true;
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
