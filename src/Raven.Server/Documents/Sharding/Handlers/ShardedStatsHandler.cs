using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Documents.Sharding.Handlers.Processors.Stats;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedStatsHandler : ShardedDatabaseRequestHandler
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
    }
}
