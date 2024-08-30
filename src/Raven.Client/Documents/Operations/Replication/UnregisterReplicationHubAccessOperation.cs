using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Replication
{
    /// <summary>
    /// Removes a previously defined hub access configuration using the UnregisterReplicationHubAccessOperation.
    /// This operation disables the specified access by removing the associated certificate and permissions from the replication hub.
    /// </summary>
    public sealed class UnregisterReplicationHubAccessOperation : IMaintenanceOperation
    {
        private readonly string _hubName;
        private readonly string _thumbprint;

        /// <inheritdoc cref="UnregisterReplicationHubAccessOperation" />
        /// <param name="hubName">The name of the replication hub from which the access configuration is being removed.</param>
        /// <param name="thumbprint">The thumbprint of the certificate associated with the access configuration to be unregistered.</param>
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

        private sealed class UnregisterReplicationHubAccessCommand : RavenCommand, IRaftCommand
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
