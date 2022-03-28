using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Refresh;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public class ShardedRefreshHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/refresh/config", "GET")]
    public async Task GetRefreshConfiguration()
    {
        using (var processor = new ShardedRefreshHandlerProcessorForGetRefreshConfiguration(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/admin/refresh/config", "POST")]
    public async Task PostRefreshConfiguration()
    {
        using (var processor = new ShardedRefreshHandlerProcessorForPostRefreshConfiguration(this))
            await processor.ExecuteAsync();
    }
}
