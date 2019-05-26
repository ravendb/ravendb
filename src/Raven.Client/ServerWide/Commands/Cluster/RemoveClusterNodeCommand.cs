using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Commands.Cluster
{
    internal class RemoveClusterNodeCommand : RavenCommand, IRaftCommand
    {
        private readonly string _node;

        public override bool IsReadRequest => false;

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

        public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
    }
}
