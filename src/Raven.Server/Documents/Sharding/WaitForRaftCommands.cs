using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding
{
    public class WaitForRaftCommands : RavenCommand
    {
        private readonly List<long> _raftIndexIds;

        public WaitForRaftCommands(List<long> raftIndexIds)
        {
            _raftIndexIds = raftIndexIds;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/rachis/wait-for-raft-commands";

            var waitForCommands = new WaitForCommandsRequest
            {
                RaftIndexIds = _raftIndexIds
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
        public List<long> RaftIndexIds { get; set; }
    }
}
