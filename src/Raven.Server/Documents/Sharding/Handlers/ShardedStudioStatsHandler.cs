extern alias NGC;
using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    internal class ShardedStudioStatsHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/studio/footer/stats", "GET")]
        public async Task FooterStats()
        {
            using (var processor = new ShardedStatsHandlerProcessorForGetStudioFooterStats(this))
            {
                await processor.ExecuteAsync();
            }
        }
    }
}
