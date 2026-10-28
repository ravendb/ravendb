using System;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Replication
{
    internal sealed class GetReplicationOngoingTasksProgressCommand : RavenCommand<IReplicationTaskProgress[]>
    {
        private readonly string[] _names;
        private readonly bool _exact;

        public GetReplicationOngoingTasksProgressCommand(string[] names, string nodeTag, bool exact = false)
        {
            _names = names;
            _exact = exact;
            SelectedNodeTag = nodeTag;
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/replication/progress";

            var separator = '?';

            if (_names is { Length: > 0 })
            {
                for (var i = 0; i < _names.Length; i++)
                {
                    url += $"{separator}name={Uri.EscapeDataString(_names[i])}";
                    separator = '&';
                }
            }

            if (_exact)
            {
                url += $"{separator}exact=true";
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
