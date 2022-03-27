using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Replication;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public class ShardedReplicationHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/replication/conflicts/solver", "GET")]
    public async Task GetConflictSolver()
    {
        using (var processor = new ShardedReplicationHandlerProcessorForGetConflictSolver(this))
            await processor.ExecuteAsync();
    }
}
