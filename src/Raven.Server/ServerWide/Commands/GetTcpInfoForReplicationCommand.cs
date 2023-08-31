using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.ServerWide.Commands;
using Sparrow.Json;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class GetTcpInfoForReplicationCommand : GetTcpInfoCommand
    {
        private readonly string _localNodeTag;

        public GetTcpInfoForReplicationCommand(string localNodeTag, string tag, string dbName = null) : base(tag, dbName)
        {
            _localNodeTag = localNodeTag;
        }

        public GetTcpInfoForReplicationCommand(string localNodeTag, string tag, string dbName, string dbId, long etag) : base(tag, dbName, dbId, etag)
        {
            _localNodeTag = localNodeTag;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = base.CreateRequest(ctx, node, out url);
            url += $"&nodeTag={_localNodeTag}";
            return request;
        }
    }
}
