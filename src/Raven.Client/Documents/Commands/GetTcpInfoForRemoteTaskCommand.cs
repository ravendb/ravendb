using System;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide.Commands;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    internal sealed class GetTcpInfoForRemoteTaskCommand : RavenCommand<TcpConnectionInfo>
    {
        private readonly string _remoteDatabase;
        private readonly string _remoteTask;
        private readonly string _changeVector;
        private readonly bool _verifyDatabase;

        public GetTcpInfoForRemoteTaskCommand(string remoteDatabase, string remoteTask, string changeVector = null, bool verifyDatabase = false)
        {
            _remoteDatabase = remoteDatabase ?? throw new ArgumentNullException(nameof(remoteDatabase));
            _remoteTask = remoteTask ?? throw new ArgumentNullException(nameof(remoteTask));
            _changeVector = changeVector;
            _verifyDatabase = verifyDatabase;
            Timeout = TimeSpan.FromSeconds(15);
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/info/remote-task/tcp?" +
                  $"database={Uri.EscapeDataString(_remoteDatabase)}" +
                  $"&remote-task={Uri.EscapeDataString(_remoteTask)}" +
                  $"&tag=wakeup"; // for backward compatibility only

            if (_verifyDatabase)
                url += "&verify-database=true";

            RequestedNode = node;
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };

            if (_changeVector != null)
                request.Headers.Add("change-vector", _changeVector);

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