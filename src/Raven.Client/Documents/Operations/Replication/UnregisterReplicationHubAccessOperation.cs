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
        private readonly string _hubDefinitionName;
        private readonly string _thumbprint;

        public UnregisterReplicationHubAccessOperation(string hubDefinitionName, string thumbprint)
        {
            if (string.IsNullOrWhiteSpace(hubDefinitionName)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(hubDefinitionName));
            if (string.IsNullOrWhiteSpace(thumbprint)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(thumbprint));
            _hubDefinitionName = hubDefinitionName;
            _thumbprint = thumbprint;
        }
        
        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new UnregisterReplicationHubAccessCommand(_hubDefinitionName, _thumbprint); 
        }

        private class UnregisterReplicationHubAccessCommand : RavenCommand, IRaftCommand
        {
            private readonly string _hubDefinitionName;
            private readonly string _thumbprint;

            public UnregisterReplicationHubAccessCommand(string hubDefinitionName, string thumbprint)
            {
                _hubDefinitionName = hubDefinitionName;
                _thumbprint = thumbprint;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/tasks/pull-replication/hub/access?name={Uri.EscapeUriString(_hubDefinitionName)}&thumbprint={Uri.EscapeUriString(_thumbprint)}";

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
