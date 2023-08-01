using System;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Commands.Cluster
{
    internal sealed class AddClusterNodeCommand : RavenCommand, IRaftCommand
    {
        private readonly string _url;
        private readonly string _tag;
        private readonly bool _watcher;

        public override bool IsReadRequest => false;

        public AddClusterNodeCommand(string url, string tag = null, bool watcher = false)
        {
            _url = url;
            _tag = tag;
            _watcher = watcher;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/cluster/node?url={Uri.EscapeDataString(_url)}&watcher={_watcher}";
            if (string.IsNullOrEmpty(_tag) == false)
            {
                url += $"&tag={_tag}";
            }
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Put
            };

            return request;
        }

        public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
    }
}
