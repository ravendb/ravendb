using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

/// <summary>
/// Sharded counterpart to <see cref="Documents.CdcSink.Handlers.CdcSinkHandler"/>. CDC sinks
/// are not supported on sharded databases as of yet, so every route here rejects callers
/// with <see cref="Raven.Client.Exceptions.Sharding.NotSupportedInShardingException"/>
/// via <see cref="NotSupportedInShardingProcessor"/>.
/// </summary>
public sealed class ShardedCdcSinkHandler : ShardedDatabaseRequestHandler
{
    private const string NotSupportedMessage = "CDC Sinks are currently not supported in sharding";

    [RavenShardedAction("/databases/*/admin/cdc-sink/test", "POST")]
    public async Task PostScriptTest()
    {
        using (var processor = new NotSupportedInShardingProcessor(this, NotSupportedMessage))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/admin/cdc-sink/schema", "POST")]
    public async Task PostSchema()
    {
        using (var processor = new NotSupportedInShardingProcessor(this, NotSupportedMessage))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/cdc-sink/performance", "GET")]
    public async Task Performance()
    {
        using (var processor = new NotSupportedInShardingProcessor(this, NotSupportedMessage))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/cdc-sink/performance/live", "GET")]
    public async Task PerformanceLive()
    {
        using (var processor = new NotSupportedInShardingProcessor(this, NotSupportedMessage))
            await processor.ExecuteAsync();
    }
}
