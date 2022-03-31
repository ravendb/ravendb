using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Sorters;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers.Admin;

public class ShardedAdminSortersHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/admin/sorters", "PUT")]
    public async Task Put()
    {
        using (var processor = new ShardedAdminSortersHandlerProcessorForPut(this))
            await processor.ExecuteAsync();
    }
}
