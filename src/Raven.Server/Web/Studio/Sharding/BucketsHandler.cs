using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.Web.Studio.Processors;

namespace Raven.Server.Web.Studio.Sharding
{
    public sealed class BucketsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/sharding/buckets", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetBuckets()
        {
            using (var processor = new BucketsHandlerProcessorForGetBuckets(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/debug/sharding/bucket", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetBucket()
        {
            using (var processor = new BucketsHandlerProcessorForGetBucket(this))
                await processor.ExecuteAsync();
        }
    }
}
