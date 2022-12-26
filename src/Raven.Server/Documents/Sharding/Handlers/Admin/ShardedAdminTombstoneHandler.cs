using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers.Admin
{
    public class ShardedAdminTombstoneHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/tombstones/cleanup", "POST")]
        public async Task Cleanup()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Tombstone cleanup."))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/tombstones/state", "GET")]
        public async Task State()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Get Tombstone state."))
                await processor.ExecuteAsync();
        }
    }
}
