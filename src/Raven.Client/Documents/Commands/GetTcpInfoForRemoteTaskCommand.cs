using System;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide.Commands;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    internal class GetTcpInfoForRemoteTaskCommand : RavenCommand<TcpConnectionInfo>
    {
        private readonly string _remoteDatabase;
        private readonly string _remoteTask;
        private readonly string _tag;
        private bool _verifyDatabase;

        public GetTcpInfoForRemoteTaskCommand(string tag, string remoteDatabase, string remoteTask, bool verifyDatabase = false)
        {
            _remoteDatabase = remoteDatabase ?? throw new ArgumentNullException(nameof(remoteDatabase));
            _remoteTask = remoteTask ?? throw new ArgumentNullException(nameof(remoteTask));
            _tag = tag;
            _verifyDatabase = verifyDatabase;
            Timeout = TimeSpan.FromSeconds(15);
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/info/remote-task/tcp?" +
                  $"database={Uri.EscapeDataString(_remoteDatabase)}" +
                  $"&remote-task={Uri.EscapeDataString(_remoteTask)}" +
                  $"&tag={Uri.EscapeDataString(_tag)}";
            if (_verifyDatabase)
            {
                url += "&verify-database=true";
            }

            RequestedNode = node;
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.TcpConnectionInfo(response);
        }

        public ServerNode RequestedNode { get; private set; }

        public override bool IsReadRequest => true;
    }
}
