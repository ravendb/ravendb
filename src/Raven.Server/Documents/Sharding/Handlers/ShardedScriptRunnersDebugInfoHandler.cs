using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public sealed class ShardedScriptRunnersDebugInfoHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/debug/script-runners", "GET")]
        public async Task GetJSDebugInfo()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Debug script-runners operations."))
                await processor.ExecuteAsync();
        }
    }
}
