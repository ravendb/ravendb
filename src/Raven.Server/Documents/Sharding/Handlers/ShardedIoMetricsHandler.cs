using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Documents.Sharding.Handlers.Processors.IoMetrics;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public sealed class ShardedIoMetricsHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/debug/io-metrics/live", "GET")]
    public async Task Live()
    {
        using (var processor = new ShardedIoMetricsHandlerProcessorForLive(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/debug/io-metrics", "GET")]
    public async Task Get()
    {
        using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Debug Information operations."))
            await processor.ExecuteAsync();
    }
}
