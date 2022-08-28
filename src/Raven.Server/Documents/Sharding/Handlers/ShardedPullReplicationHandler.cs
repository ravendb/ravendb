using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.Documents.Sharding.Handlers.Processors.Replication;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedPullReplicationHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/tasks/sink-pull-replication", "POST")]
        public async Task UpdatePullReplicationOnSinkNode()
        {
            using (var processor = new ShardedPullReplicationHandlerProcessorForUpdatePullReplicationOnSinkNode(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/tasks/pull-replication/hub", "PUT")]
        public async Task DefineHub()
        {
            using (var processor = new ShardedPullReplicationHandlerProcessorForDefineHub(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/pull-replication/generate-certificate", "POST")]
        public async Task GeneratePullReplicationCertificate()
        {
            using (var processor = new ShardedPullReplicationHandlerProcessorForGenerateCertificate(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/tasks/pull-replication/hub/access", "PUT")]
        public async Task RegisterHubAccess()
        {
            using (var processor = new ShardedPullReplicationHandlerProcessorForRegisterHubAccess(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/tasks/pull-replication/hub/access", "GET")]
        public async Task ListHubAccess()
        {
            using (var processor = new ShardedPullReplicationHandlerProcessorForGetListHubAccess(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/tasks/pull-replication/hub/access", "DELETE")]
        public async Task UnregisterHubAccess()
        {
            using (var processor = new ShardedPullReplicationHandlerProcessorForUnregisterHubAccess(this))
                await processor.ExecuteAsync();
        }
    }
}
