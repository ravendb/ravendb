using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Tcp;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public class ShardedTcpManagementHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/tcp", "GET")]
    public async Task GetAll()
    {
        using (var processor = new ShardedTcpManagementHandlerProcessorForGetAll(this))
            await processor.ExecuteAsync();
    }
}
