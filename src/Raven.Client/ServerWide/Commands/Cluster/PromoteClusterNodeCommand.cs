using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Commands.Cluster
{
    internal class PromoteClusterNodeCommand : RavenCommand, IRaftCommand
    {
        private readonly string _node;

        public override bool IsReadRequest => false;

        public PromoteClusterNodeCommand(string node)
        {
            _node = node;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/cluster/promote?nodeTag={_node}";
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post
            };
            return request;
        }

        public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
    }
}
