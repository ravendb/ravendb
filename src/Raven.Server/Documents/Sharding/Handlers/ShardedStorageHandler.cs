using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Documents.Sharding.Handlers.Processors.Debugging;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public sealed class ShardedStorageHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/debug/storage/report", "GET")]
    public async Task Report()
    {
        using (var processor = new ShardedStorageHandlerProcessorForGetReport(this))
            await processor.ExecuteAsync();
    }


    [RavenShardedAction("/databases/*/debug/storage/environment/report", "GET")]
    public async Task GetEnvironmentReport()
    {
        using (var processor = new ShardedStorageHandlerProcessorForGetEnvironmentReport(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/admin/storage/manual-flush", "POST")]
    public async Task ManualFlush()
    {
        using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Admin manual-flush operation."))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/admin/storage/manual-sync", "POST")]
    public async Task ManualSync()
    {
        using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Admin manual-sync operation."))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/debug/storage/trees", "GET")]
    public async Task Trees()
    {
        using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Debug Information operations."))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/debug/storage/btree-structure", "GET")]
    public async Task BTreeStructure()
    {
        using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Debug Information operations."))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/debug/storage/fst-structure", "GET")]
    public async Task FixedSizeTreeStructure()
    {
        using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Debug Information operations."))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/debug/storage/all-environments/report", "GET")]
    public async Task AllEnvironmentsReport()
    {
        using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Debug Information operations."))
            await processor.ExecuteAsync();
    }
}
