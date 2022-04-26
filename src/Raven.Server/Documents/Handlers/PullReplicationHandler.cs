using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class PullReplicationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/tasks/pull-replication/hub", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task DefineHub()
        {
            using (var processor = new PullReplicationHandlerProcessorForDefineHub(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/tasks/pull-replication/hub/access", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task RegisterHubAccess()
        {
            using (var processor = new PullReplicationHandlerProcessorForRegisterHubAccess(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/tasks/pull-replication/hub/access", "DELETE", AuthorizationStatus.DatabaseAdmin)]
        public async Task UnregisterHubAccess()
        {
            var hub = GetStringQueryString("name", true);
            var thumbprint = GetStringQueryString("thumbprint", true);

            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            ServerStore.LicenseManager.AssertCanAddPullReplicationAsHub();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var command = new UnregisterReplicationHubAccessCommand(Database.Name, hub, thumbprint, GetRaftRequestIdFromQuery());
                var result = await Server.ServerStore.SendToLeaderAsync(command);
                await WaitForIndexToBeAppliedAsync(context, result.Index);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName(nameof(ReplicationHubAccessResponse.RaftCommandIndex));
                    writer.WriteInteger(result.Index);
                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/databases/*/admin/tasks/pull-replication/hub/access", "GET", AuthorizationStatus.DatabaseAdmin)]
        public async Task ListHubAccess()
        {
            using (var processor = new PullReplicationHandlerProcessorForGetListHubAccess(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/tasks/sink-pull-replication", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task UpdatePullReplicationOnSinkNode()
        {
            using (var processor = new PullReplicationHandlerProcessorForUpdatePullReplicationOnSinkNode(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/pull-replication/generate-certificate", "POST", AuthorizationStatus.DatabaseAdmin, DisableOnCpuCreditsExhaustion = true)]
        public async Task GeneratePullReplicationCertificate()
        {
            using (var processor = new PullReplicationHandlerProcessorForGenerateCertificate<DatabaseRequestHandler, DocumentsOperationContext>(this, ContextPool))
                await processor.ExecuteAsync();
        }

        public class PullReplicationCertificate
        {
            public string PublicKey { get; set; }
            public string Certificate { get; set; }
            public string Thumbprint { get; set; }
        }
    }
}
