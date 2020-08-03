using System;
using System.IO;
using System.Net;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Replication
{
    public class RegisterReplicationHubAccessOperation : IMaintenanceOperation
    {
        private readonly string _hubDefinitionName;
        private readonly ReplicationHubAccess _access;

        public RegisterReplicationHubAccessOperation(string hubDefinitionName, ReplicationHubAccess access)
        {
            if (string.IsNullOrWhiteSpace(hubDefinitionName)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(hubDefinitionName));
            _hubDefinitionName = hubDefinitionName;
            _access = access ?? throw new ArgumentNullException(nameof(access));
        }
        
        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new RegisterReplicationHubAccessCommand(_hubDefinitionName, _access); 
        }

        private class RegisterReplicationHubAccessCommand : RavenCommand, IRaftCommand
        {
            private readonly string _hubName;
            private readonly ReplicationHubAccess _access;

            public RegisterReplicationHubAccessCommand(string hubName, ReplicationHubAccess access)
            {
                if (string.IsNullOrWhiteSpace(hubName)) throw new ArgumentException("Value cannot be null or whitespace.", nameof(hubName));
                _hubName = hubName;
                _access = access ?? throw new ArgumentNullException(nameof(access));
                ResponseType = RavenCommandResponseType.Raw;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/tasks/pull-replication/hub/access?name={Uri.EscapeUriString(_hubName)}";

                var blittable = ctx.ReadObject(_access.ToJson(), "register-access");

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(stream =>
                    {
                        blittable.WriteJsonTo(stream);
                    })
                };

                return request;
            }

            public override void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
            {
                using (stream)
                {
                    if (response.StatusCode == HttpStatusCode.NotFound)
                        throw new ReplicationHubNotFoundException("The replication hub " + _hubName +
                                                            " was not found on the database. Did you forget to define it first?");
                }
            }

            public string RaftUniqueRequestId { get; } =  RaftIdGenerator.NewId();
        }
    }
}
