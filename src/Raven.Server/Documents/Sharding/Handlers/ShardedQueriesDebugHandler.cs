using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Queries;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public class ShardedQueriesDebugHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/debug/queries/kill", "POST")]
    public async Task KillQuery()
    {
        using (var processor = new ShardedQueriesDebugHandlerProcessorForKillQuery(this))
            await processor.ExecuteAsync();
    }
}
