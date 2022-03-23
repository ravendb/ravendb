using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Routing;
using Raven.Server.Web.Studio.Sharding.Processors;

namespace Raven.Server.Web.Studio.Sharding;

public class ShardedStudioDatabaseTasksHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/studio-tasks/indexes/configuration/defaults", "GET")]
    public async Task GetIndexDefaults()
    {
        using (var processor = new ShardedStudioDatabaseTasksHandlerProcessorForGetIndexDefaults(this))
            await processor.ExecuteAsync();
    }
}
