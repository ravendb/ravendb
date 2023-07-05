using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Web.System.Processors.Stats;

namespace Raven.Server.Web.System
{
    public class AdminStatsHandler : ServerRequestHandler
    {
        [RavenAction("/admin/stats", "GET", AuthorizationStatus.Operator, SkipLastRequestTimeUpdate = true, IsDebugInformationEndpoint = true)]
        public async Task GetServerStatistics()
        {
            using (var processor = new AdminStatsHandlerProcessorForGetServerStatistics(this))
                await processor.ExecuteAsync();
        }
    }
}
