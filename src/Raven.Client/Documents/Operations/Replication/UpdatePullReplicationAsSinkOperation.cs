using System;
using System.Diagnostics;
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
        private readonly bool _useServerCertificate;
        
        // Kept for the binary compatibility purposes.
        public UpdatePullReplicationAsSinkOperation(PullReplicationAsSink pullReplication) : this(pullReplication, false)
        {
        }
        
        /// <inheritdoc cref="UpdatePullReplicationAsSinkOperation"/>
        /// <param name="pullReplication">
        /// The <see cref="PullReplicationAsSink"/> object containing the updated configuration for the pull replication sink task.
        /// This configuration includes details such as the source database, connection strings, allowed paths for data flow 
        /// between the sink and hub, and an optional private key for a certificate used in secure communication.
        /// </param>
        /// <param name="useServerCertificate">Makes the replication use the server certificate. Requires <see cref="PullReplicationAsSink.CertificateWithPrivateKey"/> to be null.</param>
        /// <exception cref="AuthorizationException">
        /// Thrown if the provided certificate does not include a private key but is required for secure replication.
        /// </exception>
        public UpdatePullReplicationAsSinkOperation(PullReplicationAsSink pullReplication, bool useServerCertificate = false)
        {
            _pullReplication = pullReplication;
            _useServerCertificate = useServerCertificate;

            if (pullReplication.CertificateWithPrivateKey != null)
            {
                if (useServerCertificate)
                    throw new ArgumentException(
                        $"When {nameof(useServerCertificate)} is set to true, " +
                        $"{nameof(PullReplicationAsSink.CertificateWithPrivateKey)} should be null to use server certificate.");
                
                
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
            return new UpdatePullEdgeReplication(conventions, _pullReplication, _useServerCertificate);
        }

        private class UpdatePullEdgeReplication(DocumentConventions conventions, PullReplicationAsSink pullReplication, bool useServerCertificate) : RavenCommand<ModifyOngoingTaskResult>, IRaftCommand
        {
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                pullReplication.AllowedHubToSinkPaths = PullReplicationPathFilterUtils.NormalizeAndValidate(pullReplication.AllowedHubToSinkPaths, pullReplication.Name ?? pullReplication.HubName);
                pullReplication.AllowedSinkToHubPaths = PullReplicationPathFilterUtils.NormalizeAndValidate(pullReplication.AllowedSinkToHubPaths, pullReplication.Name ?? pullReplication.HubName);
                DynamicJsonValue replication = pullReplication.ToJson();
                
                // Aligned with ServerStore.UpdatePullReplicationAsSink to not introduce breaking changes
                if (pullReplication.CertificateWithPrivateKey == null && useServerCertificate)
                {
                    int removed = replication.Properties.RemoveAll(pair => pair.Name == nameof(PullReplicationAsSink.CertificateWithPrivateKey));
                    Debug.Assert(removed > 0);
                }
                
                url = $"{node.Url}/databases/{node.Database}/admin/tasks/sink-pull-replication";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        var json = new DynamicJsonValue
                        {
                            ["PullReplicationAsSink"] = replication
                        };

                        await ctx.WriteAsync(stream, ctx.ReadObject(json, "update-pull-replication")).ConfigureAwait(false);
                    }, conventions)
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
