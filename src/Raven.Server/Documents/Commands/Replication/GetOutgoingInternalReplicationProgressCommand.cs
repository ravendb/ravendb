using System.Net.Http;
using Raven.Client.Http;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Replication
{
    internal sealed class GetOutgoingInternalReplicationProgressCommand : RavenCommand<ReplicationTaskProgress[]>
    {
        public GetOutgoingInternalReplicationProgressCommand(string nodeTag)
        {
            SelectedNodeTag = nodeTag;
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/outgoing-internal-replication/progress";

            return new HttpRequestMessage { Method = HttpMethod.Get };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                return;

            Result = JsonDeserializationServer.ReplicationTaskProgressResponse(response).Results;
        }

        internal sealed class ReplicationTaskProgressResponse
        {
            public ReplicationTaskProgress[] Results { get; set; }
        }
    }
}
