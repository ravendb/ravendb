using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Queries;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public sealed class ShardedQueriesDebugHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/debug/queries/kill", "POST")]
    public async Task KillQuery()
    {
        using (var processor = new ShardedQueriesDebugHandlerProcessorForKillQuery(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/debug/queries/running", "GET")]
    public async Task RunningQueries()
    {
        using (var processor = new ShardedQueriesDebugHandlerProcessorForRunningQueries(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/debug/queries/cache/list", "GET")]
    public async Task QueriesCacheList()
    {
        using (var processor = new ShardedQueriesDebugHandlerProcessorForQueriesCacheList(this))
            await processor.ExecuteAsync();
    }
}
