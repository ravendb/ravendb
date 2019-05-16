using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Commands.Cluster
{
    internal class RemoveClusterNodeCommand : RavenCommand
    {
        private readonly string _node;

        public override bool IsReadRequest => false;
        public override bool IsClusterCommand => true;

        public RemoveClusterNodeCommand(string node)
        {
            _node = node;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/cluster/node?nodeTag={_node}";
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Delete
            };
            return request;
        }
    }
}
