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
        private readonly List<long> _raftCommandIndexes;

        public WaitForRaftCommands(List<long> raftCommandIndexes)
        {
            _raftCommandIndexes = raftCommandIndexes;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/rachis/wait-for-raft-commands";

            var waitForCommands = new WaitForCommandsRequest
            {
                RaftCommandIndexes = _raftCommandIndexes
            };

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                    await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(waitForCommands, ctx)).ConfigureAwait(false))
            };

            return request;
        }
    }

    public class WaitForCommandsRequest
    {
        public List<long> RaftCommandIndexes { get; set; }
    }
}
