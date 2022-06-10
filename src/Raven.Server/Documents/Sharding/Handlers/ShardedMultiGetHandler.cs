using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.MultiGet;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public class ShardedMultiGetHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/multi_get", "POST")]
    public async Task Post()
    {
        using (var processor = new ShardedMultiGetHandlerProcessorForPost(this))
            await processor.ExecuteAsync();
    }
}
