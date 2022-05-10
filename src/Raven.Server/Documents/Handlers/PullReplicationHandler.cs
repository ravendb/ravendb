using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.Routing;

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
            using (var processor = new PullReplicationHandlerProcessorForUnregisterHubAccess(this))
                await processor.ExecuteAsync();
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
            using (var processor = new PullReplicationHandlerProcessorForGenerateCertificate<DatabaseRequestHandler>(this))
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
