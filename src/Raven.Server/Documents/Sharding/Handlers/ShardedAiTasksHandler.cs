using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.ETL;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public sealed class ShardedAiTasksHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/ai/errors", "GET")]
    public async Task GetErrors()
    {
        using (var processor = new ShardedAiTasksHandlerProcessorForGetErrors(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/ai/errors", "DELETE")]
    public async Task DeleteErrors()
    {
        using (var processor = new ShardedAiTasksHandlerProcessorForDeleteErrors(this))
            await processor.ExecuteAsync();
    }
}
