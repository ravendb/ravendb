using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public sealed class ShardedPerformanceMetricsHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/debug/perf-metrics", "GET")]
        public async Task IoMetrics()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Debug Information operations."))
                await processor.ExecuteAsync();
        }
    }
}
