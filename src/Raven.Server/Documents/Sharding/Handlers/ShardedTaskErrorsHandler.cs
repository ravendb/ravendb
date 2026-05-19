using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.ETL;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public sealed class ShardedTaskErrorsHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/tasks/errors", "GET")]
    public async Task GetAllErrors()
    {
        using (var processor = new ShardedTaskErrorsHandlerProcessorForGetAllErrors(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/tasks/errors", "DELETE")]
    public async Task DeleteErrors()
    {
        using (var processor = new ShardedTaskErrorsHandlerProcessorForDeleteErrors(this))
            await processor.ExecuteAsync();
    }
}
