using System.Threading.Tasks;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Web.System
{
    public class AdminStatsHandler : RequestHandler
    {
        [RavenAction("/admin/stats", "GET", AuthorizationStatus.Operator, SkipLastRequestTimeUpdate = true, IsDebugInformationEndpoint = true)]
        public async Task GetRootStats()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                Server.Statistics.WriteTo(writer);
            }
        }
    }
}
