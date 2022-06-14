using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Batches;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers.Batches
{
    public class ShardedBatchHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/bulk_docs", "POST")]
        public async Task BulkDocs()
        {
            using (var processor = new ShardedBatchHandlerProcessorForBulkDocs(this))
                await processor.ExecuteAsync();
        }
    }
}
