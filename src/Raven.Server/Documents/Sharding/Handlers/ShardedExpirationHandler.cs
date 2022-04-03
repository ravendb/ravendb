using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Expiration;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedExpirationHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/expiration/config", "GET")]
        public async Task GetExpirationConfig()
        {
            using (var processor = new ShardedExpirationHandlerProcessorForGet(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/expiration/config", "POST")]
        public async Task ConfigExpiration()
        {
            using (var processor = new ShardedExpirationHandlerProcessorForPost(this))
                await processor.ExecuteAsync();
        }
    }
}
