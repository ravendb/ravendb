using System.Net;
using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class PullReplicationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/tasks/pull-replication/hub", "PUT", AuthorizationStatus.Operator)]
        public async Task DefineHub()
        {
            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            ServerStore.LicenseManager.AssertCanAddPullReplicationAsHub();

            PullReplicationDefinition pullReplication = null;
            await DatabaseConfigurations((_, databaseName, blittableJson, guid) =>
                {
                    pullReplication = JsonDeserializationClient.PullReplicationDefinition(blittableJson);
                    
                    pullReplication.Validate(ServerStore.Server.Certificate?.Certificate != null);
                    var updatePullReplication = new UpdatePullReplicationAsHubCommand(databaseName, guid)
                    {
                        Definition = pullReplication
                    };
                    return ServerStore.SendToLeaderAsync(updatePullReplication);
                }, "update-hub-pull-replication", 
                GetRaftRequestIdFromQuery(),
                fillJson: (json, _, index) =>
                {
                    json[nameof(OngoingTask.TaskId)] = pullReplication.TaskId == 0 ? index : pullReplication.TaskId;
                }, statusCode: HttpStatusCode.Created);
        }
        
        [RavenAction("/databases/*/admin/tasks/pull-replication/hub/access", "PUT", AuthorizationStatus.Operator)]
        public async Task RegisterHubAccess()
        {
            var hub = GetStringQueryString("hub", true);
            
            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);
            
            ServerStore.LicenseManager.AssertCanAddPullReplicationAsHub();
            
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var blittableJson = await context.ReadForMemoryAsync(RequestBodyStream(), "register-hub-access");
                var access = JsonDeserializationClient.ReplicationHubAccess(blittableJson);
                access.Validate();

                var definition = Database.GetPullReplicationDefinition(hub);
                if (definition == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                if (definition.Certificates != null && definition.Certificates.Count > 0)
                {
                    // this handles the backward compact aspect
                    throw new InvalidOperationException("Cannot register hub access to a replication hub that already has inline certificates: " + hub  +
                                                        ". Create a new replication hub and try again");
                }

                using var cert = new X509Certificate2(Convert.FromBase64String(access.CertificateBas64));
                var publicKeyPinningHash = cert.GetPublicKeyPinningHash();

                var command = new RegisterReplicationHubAccessCommand(Database.Name, hub, access, publicKeyPinningHash, cert.Thumbprint, GetRaftRequestIdFromQuery(),
                    cert.Issuer, cert.Subject,cert.NotBefore, cert.NotAfter);
                var result = await Server.ServerStore.SendToLeaderAsync(command);
                await WaitForIndexToBeApplied(context, result.Index);
            }
        }

        [RavenAction("/databases/*/admin/tasks/pull-replication/hub/access", "DELETE", AuthorizationStatus.Operator)]
        public async Task UnregisterHubAccess()
        {
            var hub = GetStringQueryString("hub", true);
            var thumbprint = GetStringQueryString("thumbprint", true);

            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            ServerStore.LicenseManager.AssertCanAddPullReplicationAsHub();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var command = new UnregisterReplicationHubAccessCommand(Database.Name, hub, thumbprint, GetRaftRequestIdFromQuery());
                var result = await Server.ServerStore.SendToLeaderAsync(command);
                await WaitForIndexToBeApplied(context, result.Index);
            }
        }

        [RavenAction("/databases/*/admin/tasks/pull-replication/hub/access", "GET", AuthorizationStatus.Operator)]
        public Task ListHubAccess()
        {
            var hub = GetStringQueryString("hub", true);
            int pageSize = GetPageSize();
            var start = GetStart();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using(context.OpenReadTransaction())
            {
                var results = Server.ServerStore.Cluster.GetReplicationHubCertificateByHub(context, Database.Name, hub, start, pageSize);
             
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WriteArray(nameof(ReplicationHubAccessList.Results), results);
                    
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(ReplicationHubAccessList.Skip));
                    writer.WriteInteger(start);

                    writer.WriteEndObject();
                }
                
                return Task.CompletedTask;
            }
        }

        [RavenAction("/databases/*/admin/tasks/sink-pull-replication", "POST", AuthorizationStatus.Operator)]
        public async Task UpdatePullReplicationOnSinkNode()
        {
            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            ServerStore.LicenseManager.AssertCanAddPullReplicationAsSink();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                PullReplicationAsSink pullReplication = null;
                await DatabaseConfigurations(
                    (_, databaseName, blittableJson, guid) => ServerStore.UpdatePullReplicationAsSink(databaseName, blittableJson, guid, out pullReplication),
                    "update-sink-pull-replication", GetRaftRequestIdFromQuery(),
                    fillJson: (json, _, index) =>
                    {
                        using (context.OpenReadTransaction())
                        {
                            var topology = ServerStore.Cluster.ReadDatabaseTopology(context, Database.Name);
                            json[nameof(OngoingTask.ResponsibleNode)] = Database.WhoseTaskIsIt(topology, pullReplication, null);
                        }

                        json[nameof(ModifyOngoingTaskResult.TaskId)] = pullReplication.TaskId == 0 ? index : pullReplication.TaskId;
                    }, statusCode: HttpStatusCode.Created);
            }
        }

        [RavenAction("/databases/*/admin/pull-replication/generate-certificate", "POST", AuthorizationStatus.Operator, DisableOnCpuCreditsExhaustion =true)]
        public Task GeneratePullReplicationCertificate()
        {
            if (ServerStore.Server.Certificate?.Certificate == null)
                throw new BadRequestException("This endpoint requires secured server.");

            ServerStore.LicenseManager.AssertCanAddPullReplicationAsHub();

            var validYears = GetIntValueQueryString("validYears", required: false) ?? 0; // 0 yr. will set the expiration to 3 months
            var notAfter = validYears == 0 ? DateTime.UtcNow.AddMonths(3) : DateTime.UtcNow.AddYears(validYears);

            var log = new StringBuilder();
            var commonNameValue = "PullReplicationAutogeneratedCertificate";
            CertificateUtils.CreateCertificateAuthorityCertificate(commonNameValue + " CA", out var ca, out var caSubjectName, log);
            CertificateUtils.CreateSelfSignedCertificateBasedOnPrivateKey(commonNameValue, caSubjectName, ca, false, false, notAfter, out var certBytes, log: log);
            var certificateWithPrivateKey = new X509Certificate2(certBytes, (string)null, X509KeyStorageFlags.MachineKeySet | X509KeyStorageFlags.Exportable);
            certificateWithPrivateKey.Verify();

            var keyPairInfo = new PullReplicationCertificate
            {
                PublicKey = Convert.ToBase64String(certificateWithPrivateKey.Export(X509ContentType.Cert)),
                Thumbprint = certificateWithPrivateKey.Thumbprint,
                Certificate = Convert.ToBase64String(certificateWithPrivateKey.Export(X509ContentType.Pfx))
            };

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(keyPairInfo.PublicKey));
                writer.WriteString(keyPairInfo.PublicKey);
                writer.WriteComma();

                writer.WritePropertyName(nameof(keyPairInfo.Certificate));
                writer.WriteString(keyPairInfo.Certificate);
                writer.WriteComma();

                writer.WritePropertyName(nameof(keyPairInfo.Thumbprint));
                writer.WriteString(keyPairInfo.Thumbprint);

                writer.WriteEndObject();
            }
            
            return Task.CompletedTask;
        }

        public class PullReplicationCertificate
        {
            public string PublicKey { get; set; }
            public string Certificate { get; set; }
            public string Thumbprint { get; set; }
        }
    }
}
