using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Replication
{
    public class UnregisterReplicationHubAccessOperation : IMaintenanceOperation
    {
        private readonly string _hubName;
        private readonly string _thumbprint;

        public UnregisterReplicationHubAccessOperation(string hubName, string thumbprint)
        {
            if (string.IsNullOrWhiteSpace(hubName))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(hubName));
            if (string.IsNullOrWhiteSpace(thumbprint))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(thumbprint));
            _hubName = hubName;
            _thumbprint = thumbprint;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new UnregisterReplicationHubAccessCommand(_hubName, _thumbprint);
        }

        private class UnregisterReplicationHubAccessCommand : RavenCommand, IRaftCommand
        {
            private readonly string _hubName;
            private readonly string _thumbprint;

            public UnregisterReplicationHubAccessCommand(string hubName, string thumbprint)
            {
                _hubName = hubName;
                _thumbprint = thumbprint;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/tasks/pull-replication/hub/access?name={Uri.EscapeDataString(_hubName)}&thumbprint={Uri.EscapeDataString(_thumbprint)}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Delete,
                };

                return request;
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
