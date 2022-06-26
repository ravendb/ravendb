using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Debugging;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public class ShardedStorageHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/debug/storage/report", "GET")]
    public async Task Report()
    {
        using (var processor = new ShardedStorageHandlerProcessorForGetReport(this))
            await processor.ExecuteAsync();
    }
}
