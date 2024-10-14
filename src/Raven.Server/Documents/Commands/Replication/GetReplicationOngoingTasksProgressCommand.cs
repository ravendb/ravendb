using System;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Replication
{
    internal sealed class GetReplicationOngoingTasksProgressCommand : RavenCommand<ReplicationTaskProgress[]>
    {
        private readonly string[] _names;

        public GetReplicationOngoingTasksProgressCommand(string[] names, string nodeTag)
        {
            _names = names;
            SelectedNodeTag = nodeTag;
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/replication/progress";

            if (_names is { Length: > 0 })
            {
                for (var i = 0; i < _names.Length; i++)
                    url += $"{(i == 0 ? "?" : "&")}name={Uri.EscapeDataString(_names[i])}";
            }

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
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
