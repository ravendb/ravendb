using System.Threading.Tasks;
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
    }
}
