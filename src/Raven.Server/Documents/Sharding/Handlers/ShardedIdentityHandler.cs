using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Identities;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedIdentityHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/identity/seed", "POST")]
        public async Task SeedIdentityFor()
        {
            using (var processor = new ShardedIdentityHandlerProcessorForPostIdentity(this))
                await processor.ExecuteAsync();
        }
    }
}
