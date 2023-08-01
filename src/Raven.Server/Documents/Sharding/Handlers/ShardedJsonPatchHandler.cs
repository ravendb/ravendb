using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public sealed class ShardedJsonPatchHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/json-patch", "PATCH")]
        public async Task DocOperations()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support json-patch."))
                await processor.ExecuteAsync();
        }
    }
}
