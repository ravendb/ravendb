using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Routing;
using Raven.Server.Web.Studio.Sharding.Processors;

namespace Raven.Server.Web.Studio.Sharding
{
    public sealed class ShardedBucketsHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/debug/sharding/buckets", "GET")]
        public async Task GetBuckets()
        {
            using (var processor = new ShardedBucketsHandlerProcessorForGetBuckets(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/debug/sharding/bucket", "GET")]
        public async Task GetBucket()
        {
            using (var processor = new ShardedBucketsHandlerProcessorForGetBucket(this))
                await processor.ExecuteAsync();
        }
    }
}
