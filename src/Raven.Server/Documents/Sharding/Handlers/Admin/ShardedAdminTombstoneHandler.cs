using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Tombstones;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers.Admin
{
    public sealed class ShardedAdminTombstoneHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/tombstones/cleanup", "POST")]
        public async Task Cleanup()
        {
            using (var processor = new ShardedAdminTombstoneHandlerProcessorForCleanup(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/tombstones/state", "GET")]
        public async Task State()
        {
            using (var processor = new ShardedAdminTombstoneHandlerProcessorForState(this))
                await processor.ExecuteAsync();
        }
    }
}
