using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors.Studio;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Web.Studio
{
    public sealed class StudioStatsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/studio/footer/stats", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetFooterStats()
        {
            using (var processor = new StudioStatsHandlerProcessorForGetFooterStats(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenAction("/databases/*/studio/license/limits-usage", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetLicenseLimitsUsage()
        {
            using (var processor = new StudioStatsHandlerProcessorForGetLicenseLimitsUsage<DocumentsOperationContext>(this))
                await processor.ExecuteAsync();
        }
    }
}
