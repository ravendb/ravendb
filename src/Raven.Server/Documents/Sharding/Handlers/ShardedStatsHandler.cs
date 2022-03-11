using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedStatsHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/stats", "GET")]
        public async Task Stats()
        {
            using (var processor = new ShardedStatsHandlerProcessorForGetDatabaseStatistics(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/stats/detailed", "GET")]
        public async Task DetailedStats()
        {
            using (var processor = new ShardedStatsHandlerProcessorForGetDetailedDatabaseStatistics(this))
                await processor.ExecuteAsync();
        }
    }

}
