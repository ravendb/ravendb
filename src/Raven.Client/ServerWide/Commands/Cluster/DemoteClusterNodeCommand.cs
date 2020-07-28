using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Commands.Cluster
{
    internal class DemoteClusterNodeCommand : RavenCommand, IRaftCommand
    {
        private readonly string _node;

        public override bool IsReadRequest => false;

        public DemoteClusterNodeCommand(string node)
        {
            _node = node;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/cluster/demote?nodeTag={_node}";
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post
            };
            return request;
        }

        public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
    }
}
