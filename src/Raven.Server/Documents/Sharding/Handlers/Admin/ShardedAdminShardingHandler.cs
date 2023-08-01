using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers.Admin
{
    public sealed class ShardedAdminShardingHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/sharding/resharding/cleanup", "POST")]
        public async Task ExecuteMoveDocuments()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support documents migration operation. " +
                                                                             "This operation is available only from a specific shard"))
                await processor.ExecuteAsync();
        }
    }
}
