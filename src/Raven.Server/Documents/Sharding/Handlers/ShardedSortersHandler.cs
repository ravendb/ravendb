using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Sorters;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public class ShardedSortersHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/sorters", "GET")]
    public async Task Get()
    {
        using (var processor = new ShardedSortersHandlerProcessorForGet(this))
            await processor.ExecuteAsync();
    }
}
