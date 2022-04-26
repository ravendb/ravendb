using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Admin.Processors.TimeSeries;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminTimeSeriesHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/timeseries/policy", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task AddTimeSeriesPolicy()
        {
            using (var processor = new AdminTimeSeriesHandlerProcessorForPutTimeSeriesPolicy(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/timeseries/policy", "DELETE", AuthorizationStatus.DatabaseAdmin)]
        public async Task RemoveTimeSeriesPolicy()
        {
            using (var processor = new AdminTimeSeriesHandlerProcessorForDeleteTimeSeriesPolicy(this))
                await processor.ExecuteAsync();
        }
    }
}
