using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.ETL;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public class ShardedEtlHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/etl/stats", "GET")]
    public async Task Stats()
    {
        using (var processor = new ShardedEtlHandlerProcessorForStats(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/etl/debug/stats", "GET")]
    public async Task DebugStats()
    {
        using (var processor = new ShardedEtlHandlerProcessorForDebugStats(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/etl/performance", "GET")]
    public async Task Performance()
    {
        using (var processor = new ShardedEtlHandlerProcessorForPerformance(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/etl/performance/live", "GET")]
    public async Task PerformanceLive()
    {
        using (var processor = new ShardedEtlHandlerProcessorForPerformanceLive(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/etl/progress", "GET")]
    public async Task Progress()
    {
        using (var processor = new ShardedEtlHandlerProcessorForProgress(this))
            await processor.ExecuteAsync();
    }
}
