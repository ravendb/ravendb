using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Replication
{
    public class GetReplicationHubAccessOperation : IMaintenanceOperation<ReplicationHubAccessResult>
    {
        private readonly string _hubDefinitionName;
        private readonly int _start;
        private readonly int _pageSize;

        public GetReplicationHubAccessOperation(string hubDefinitionName, int start = 0, int pageSize = 25)
        {
            _hubDefinitionName = hubDefinitionName;
            _start = start;
            _pageSize = pageSize;
        }
        
        public RavenCommand<ReplicationHubAccessResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetReplicationHubAccessCommand(_hubDefinitionName, _start, _pageSize); 
        }

        private class GetReplicationHubAccessCommand : RavenCommand<ReplicationHubAccessResult>
        {
            private readonly string _hubDefinitionName;
            private readonly int _start;
            private readonly int _pageSize;

            public GetReplicationHubAccessCommand(string hubDefinitionName, int start, int pageSize)
            {
                if (string.IsNullOrWhiteSpace(hubDefinitionName)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(hubDefinitionName));
                _hubDefinitionName = hubDefinitionName;
                _start = start;
                _pageSize = pageSize;
            }

            public override bool IsReadRequest { get; } = true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/tasks/pull-replication/hub/access?name={Uri.EscapeUriString(_hubDefinitionName)}&start={_start}&pageSize={_pageSize}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get,
                };

                return request;
            }
            
            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ReplicationHubAccessList(response);
            }
        }
    }
}
