extern alias NGC;
using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Documents.Sharding.Handlers.Processors.Studio;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    internal class ShardedStudioStatsHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/studio/footer/stats", "GET")]
        public async Task GetFooterStats()
        {
            using (var processor = new ShardedStudioStatsHandlerProcessorForGetFooterStats(this))
            {
                await processor.ExecuteAsync();
            }
        }
    }
}
