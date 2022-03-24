using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands
{
    public class WaitForRaftCommands : RavenCommand
    {
        private readonly List<long> _indexes;

        public WaitForRaftCommands(List<long> indexes)
        {
            _indexes = indexes;
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/rachis/wait-for-raft-commands";

            var waitForCommands = new WaitForCommandsRequest
            {
                RaftCommandIndexes = _indexes
            };

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                    {
                        writer.WriteStartObject();
                        writer.WriteArray(nameof(WaitForCommandsRequest.RaftCommandIndexes), _indexes);
                        writer.WriteEndObject();
                    }
                })
            };

            return request;
        }
    }
}
