using System.Net.Http;
using Raven.Client.Http;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Replication
{
    internal sealed class GetOutgoingInternalReplicationProgressCommand : RavenCommand<IReplicationTaskProgress[]>
    {
        private readonly bool _exact;

        public GetOutgoingInternalReplicationProgressCommand(string nodeTag, bool exact = false)
        {
            _exact = exact;
            SelectedNodeTag = nodeTag;
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/replication/internal/outgoing/progress";

            if (_exact)
            {
                url += "?exact=true";
            }

            return new HttpRequestMessage { Method = HttpMethod.Get };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                return;

            Result = JsonDeserializationServer.InternalReplicationTaskProgressResponse(response).Results;
        }

        internal sealed class InternalReplicationTaskProgressResponse
        {
            public InternalReplicationTaskProgress[] Results { get; set; }
        }
    }
}
