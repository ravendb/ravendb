using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Admin.Processors.TimeSeries;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedAdminTimesSeriesHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/timeseries/policy", "PUT")]
        public async Task AddTimeSeriesPolicy()
        {
            using (var processor = new ShardedAdminTimeSeriesHandlerProcessorForPutTimeSeriesPolicy(this))
                await processor.ExecuteAsync();
        }
    }
}
