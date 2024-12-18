using System;
using System.Net.Http;
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
    /// <summary>
    /// Operation to update the configuration of a pull replication task as a sink.
    /// Pull replication as a sink allows a database to pull data from a source (hub) database in another cluster or server.
    /// </summary>
    public sealed class UpdatePullReplicationAsSinkOperation : IMaintenanceOperation<ModifyOngoingTaskResult>
    {
        private readonly PullReplicationAsSink _pullReplication;

        /// <inheritdoc cref="UpdatePullReplicationAsSinkOperation"/>
        /// <param name="pullReplication">
        /// The <see cref="PullReplicationAsSink"/> object containing the updated configuration for the pull replication sink task.
        /// This configuration includes details such as the source database, connection strings, allowed paths for data flow 
        /// between the sink and hub, and an optional private key for a certificate used in secure communication.
        /// </param>
        /// <exception cref="AuthorizationException">
        /// Thrown if the provided certificate does not include a private key but is required for secure replication.
        /// </exception>
        public UpdatePullReplicationAsSinkOperation(PullReplicationAsSink pullReplication)
        {
            _pullReplication = pullReplication;

            if (pullReplication.CertificateWithPrivateKey != null)
            {
                var certBytes = Convert.FromBase64String(pullReplication.CertificateWithPrivateKey);
                using (var certificate = CertificateLoaderUtil.CreateCertificate(certBytes,
                    pullReplication.CertificatePassword,
                    CertificateLoaderUtil.FlagsForExport))
                {
                    if (certificate.HasPrivateKey == false)
                        throw new AuthorizationException("Certificate with private key is required");
                }
            }
        }

        public RavenCommand<ModifyOngoingTaskResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new UpdatePullEdgeReplication(conventions, _pullReplication);
        }

        private sealed class UpdatePullEdgeReplication : RavenCommand<ModifyOngoingTaskResult>, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly PullReplicationAsSink _pullReplication;

            public UpdatePullEdgeReplication(DocumentConventions conventions, PullReplicationAsSink pullReplication)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
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
                    }, _conventions)
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
