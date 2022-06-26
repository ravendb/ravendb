using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.IoMetrics;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public class ShardedIoMetricsHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/debug/io-metrics/live", "GET")]
    public async Task Live()
    {
        using (var processor = new ShardedIoMetricsHandlerProcessorForLive(this))
            await processor.ExecuteAsync();
    }
}
