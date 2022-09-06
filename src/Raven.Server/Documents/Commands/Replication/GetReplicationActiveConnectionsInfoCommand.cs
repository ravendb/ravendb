using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.Documents.Replication.Stats;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Replication
{
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

            var outgoingConnectionsInfo = new List<ReplicationActiveConnectionsPreview.OutgoingConnectionInfo>();
            if (response.TryGet(nameof(ReplicationActiveConnectionsPreview.OutgoingConnections), out bjra))
            {
                foreach (BlittableJsonReaderObject bjro in bjra)
                {
                    var outgoingConnectionInfo = ReplicationActiveConnectionsPreview.OutgoingConnectionInfo.FromJson(bjro);
                    outgoingConnectionsInfo.Add(outgoingConnectionInfo);
                }
            }

            Result = new ReplicationActiveConnectionsPreview { IncomingConnections = incomingConnectionsInfo, OutgoingConnections = outgoingConnectionsInfo };
        }

        public override bool IsReadRequest => true;
    }
}
