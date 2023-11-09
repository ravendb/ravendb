using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Batches;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers.Batches
{
    public sealed class ShardedBatchHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/bulk_docs", "POST")]
        public async Task BulkDocs()
        {
            // if sharded :
            // this.DatabaseContext.LastIndex;
            //else
            // Database.index

            // DatabaseContext

            // var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(DatabaseName);

            using (var processor = new ShardedBatchHandlerProcessorForBulkDocs(this))
                await processor.ExecuteAsync();
        }
    }
}
