using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Collections;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedCollectionHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/collections/stats", "GET")]
        public async Task GetCollectionStats()
        {
            using (var processor = new ShardedCollectionsHandlerProcessorForGetCollectionStats(this, detailed: false))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/collections/stats/detailed", "GET")]
        public async Task GetDetailedCollectionStats()
        {
            using (var processor = new ShardedCollectionsHandlerProcessorForGetCollectionStats(this, detailed: true))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/collections/docs", "GET")]
        public async Task GetCollectionDocuments()
        {
            using (var processor = new ShardedCollectionsHandlerProcessorForGetCollectionDocuments(this))
                await processor.ExecuteAsync();
        }
    }
}
