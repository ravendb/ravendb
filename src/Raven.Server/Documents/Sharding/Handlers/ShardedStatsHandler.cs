using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Stats;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public sealed class ShardedStatsHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/stats/essential", "GET")]
        public async Task EssentialStats()
        {
            using (var processor = new ShardedStatsHandlerProcessorForEssentialStats(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/stats/detailed", "GET")]
        public async Task DetailedStats()
        {
            using (var processor = new ShardedStatsHandlerProcessorForGetDetailedDatabaseStatistics(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/stats", "GET")]
        public async Task Stats()
        {
            using (var processor = new ShardedStatsHandlerProcessorForGetDatabaseStatistics(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/healthcheck", "GET")]
        public async Task DatabaseHealthCheck()
        {
            using (var processor = new ShardedStatsHandlerProcessorForDatabaseHealthCheck(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/metrics", "GET")]
        public async Task Metrics()
        {
            using (var processor = new ShardedStatsHandlerProcessorForGetMetrics(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/metrics/puts", "GET")]
        public async Task PutsMetrics()
        {
            using (var processor = new ShardedStatsHandlerProcessorForGetMetricsPuts(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/metrics/bytes", "GET")]
        public async Task BytesMetrics()
        {
            using (var processor = new ShardedStatsHandlerProcessorForGetMetricsBytes(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/validate-unused-ids", "POST")]
        public async Task ValidateUnusedIds()
        {
            using (var processor = new ShardedStatsHandlerProcessorForPostValidateUnusedIds(this))
                await processor.ExecuteAsync();
        }
    }
}
