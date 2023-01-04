using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Routing;
using Raven.Server.Web.Studio.Sharding.Processors;

namespace Raven.Server.Web.Studio.Sharding
{
    public class ShardedBucketsHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/debug/buckets", "GET")]
        public async Task GetBuckets()
        {
            using (var processor = new ShardedBucketsHandlerProcessorForGetBuckets(this))
                await processor.ExecuteAsync();
        }
    }
}
