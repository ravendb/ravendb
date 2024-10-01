using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Revisions;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public sealed class ShardedRevisionsBinCleanerHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/revisions-bin-cleaner/config", "GET")]
        public async Task GetRevisionsBinConfig()
        {
            using (var processor = new ShardedRevisionsBinCleanerHandlerProcessorForGetConfiguration(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/revisions-bin-cleaner/config", "POST")]
        public async Task ConfigRevisionsBinCleaner()
        {
            using (var processor = new ShardedRevisionsBinCleanerHandlerProcessorForPostConfiguration(this))
                await processor.ExecuteAsync();
        }
    }
}

