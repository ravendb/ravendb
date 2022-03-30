using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Identities;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedIdentityDebugHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/debug/identities", "GET")]
        public async Task GetIdentities()
        {
            using (var processor = new ShardedIdentityDebugHandlerProcessorForGetIdentities(this))
                await processor.ExecuteAsync();
        }
    }
}
