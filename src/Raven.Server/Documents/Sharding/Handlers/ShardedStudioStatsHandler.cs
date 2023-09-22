extern alias NGC;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Studio;
using Raven.Server.Documents.Sharding.Handlers.Processors.Studio;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers
{
    internal sealed class ShardedStudioStatsHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/studio/footer/stats", "GET")]
        public async Task GetFooterStats()
        {
            using (var processor = new ShardedStudioStatsHandlerProcessorForGetFooterStats(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenShardedAction("/databases/*/studio/license/limits-usage", "GET")]
        public async Task GetLicenseLimitsUsage()
        {
            using (var processor = new StudioStatsHandlerProcessorForGetLicenseLimitsUsage<TransactionOperationContext>(this))
                await processor.ExecuteAsync();
        }
    }
}
