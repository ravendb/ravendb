using System;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions.Security;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public class UpdatePullReplicationAsSinkOperation : IMaintenanceOperation<ModifyOngoingTaskResult>
    {
        private readonly PullReplicationAsSink _pullReplication;

        public UpdatePullReplicationAsSinkOperation(PullReplicationAsSink pullReplication)
        {
            _pullReplication = pullReplication;

            if (pullReplication.CertificateWithPrivateKey != null)
            {
                var certBytes = Convert.FromBase64String(pullReplication.CertificateWithPrivateKey);
                using (var certificate = new X509Certificate2(certBytes,
                    pullReplication.CertificatePassword,
                    X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet))
                {
                    if (certificate.HasPrivateKey == false)
                        throw new AuthorizationException("Certificate with private key is required");
                }
            }
        }

        public RavenCommand<ModifyOngoingTaskResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new UpdatePullEdgeReplication(_pullReplication);
        }

        private class UpdatePullEdgeReplication : RavenCommand<ModifyOngoingTaskResult>, IRaftCommand
        {
            private readonly PullReplicationAsSink _pullReplication;

            public UpdatePullEdgeReplication(PullReplicationAsSink pullReplication)
            {
                _pullReplication = pullReplication ?? throw new ArgumentNullException(nameof(pullReplication));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/tasks/sink-pull-replication";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        var json = new DynamicJsonValue
                        {
                            ["PullReplicationAsSink"] = _pullReplication.ToJson()
                        };

                        await ctx.WriteAsync(stream, ctx.ReadObject(json, "update-pull-replication")).ConfigureAwait(false);
                    })
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ModifyOngoingTaskResult(response);
            }

            public override bool IsReadRequest => false;
            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
