using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Web.Studio
{
    public class StudioStatsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/studio/footer/stats", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task FooterStats()
        {
            using (var processor = new StatsHandlerProcessorForGetStudioFooterStats(this))
            {
                await processor.ExecuteAsync();
            }
        }
    }
}
