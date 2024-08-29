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
    /// <summary>
    /// Defines a Hub Access using the RegisterReplicationHubAccessOperation, 
    /// and configures it with the provided ReplicationHubAccess class.
    /// </summary>
    public sealed class RegisterReplicationHubAccessOperation : IMaintenanceOperation
    {
        private readonly string _hubName;
        private readonly ReplicationHubAccess _access;

        /// <inheritdoc cref="RegisterReplicationHubAccessOperation" />
        /// <param name="hubName">The name of the replication hub for which access is being defined.</param>
        /// <param name="access">The ReplicationHubAccess object that contains the configuration for the hub access.</param>
        public RegisterReplicationHubAccessOperation(string hubName, ReplicationHubAccess access)
        {
            if (string.IsNullOrWhiteSpace(hubName))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(hubName));
            _hubName = hubName;
            _access = access ?? throw new ArgumentNullException(nameof(access));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new RegisterReplicationHubAccessCommand(conventions, _hubName, _access);
        }

        private sealed class RegisterReplicationHubAccessCommand : RavenCommand, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly string _hubName;
            private readonly ReplicationHubAccess _access;

            public RegisterReplicationHubAccessCommand(DocumentConventions conventions, string hubName, ReplicationHubAccess access)
            {
                if (string.IsNullOrWhiteSpace(hubName))
                    throw new ArgumentException("Value cannot be null or whitespace.", nameof(hubName));
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _hubName = hubName;
                _access = access ?? throw new ArgumentNullException(nameof(access));
                ResponseType = RavenCommandResponseType.Raw;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/tasks/pull-replication/hub/access?name={Uri.EscapeDataString(_hubName)}";

                var blittable = ctx.ReadObject(_access.ToJson(), "register-access");

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(async stream => await blittable.WriteJsonToAsync(stream).ConfigureAwait(false), _conventions)
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

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
