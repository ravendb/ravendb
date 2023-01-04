using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.Web.Studio.Processors;

namespace Raven.Server.Web.Studio.Sharding
{
    public class BucketsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/buckets", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetBuckets()
        {
            using (var processor = new BucketsHandlerProcessorForGetBuckets(this))
                await processor.ExecuteAsync();
        }
    }
}
