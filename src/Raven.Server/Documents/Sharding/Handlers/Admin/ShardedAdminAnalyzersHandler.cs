using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Analyzers;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers.Admin
{
    public class ShardedAdminAnalyzersHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/analyzers", "PUT")]
        public async Task Put()
        {
            using (var processor = new ShardedAdminAnalyzersHandlerProcessorForPut(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/analyzers", "DELETE")]
        public async Task Delete()
        {
            using (var processor = new ShardedAdminAnalyzersHandlerProcessorForDelete(this))
                await processor.ExecuteAsync();
        }
    }
}
