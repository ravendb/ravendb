using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.ETL;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public sealed class ShardedEtlHandler : ShardedDatabaseRequestHandler
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
    
    [RavenShardedAction("/databases/*/etl/errors", "GET")]
    public async Task GetErrors()
    {
        using (var processor = new ShardedEtlHandlerProcessorForGetErrors(this))
            await processor.ExecuteAsync();
    }
    
    [RavenShardedAction("/databases/*/etl/errors", "DELETE")]
    public async Task DeleteErrors()
    {
        using (var processor = new ShardedEtlHandlerProcessorForDeleteErrors(this))
            await processor.ExecuteAsync();
    }
    
    [RavenShardedAction("/databases/*/etl/retry-batch", "POST")]
    public async Task RetryBatch()
    {
        using (var processor = new ShardedEtlHandlerProcessorForRetryBatch(this))
            await processor.ExecuteAsync();
    }
}
