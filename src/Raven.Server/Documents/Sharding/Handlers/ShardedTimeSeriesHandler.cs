using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Configuration;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedTimeSeriesHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/timeseries/config", "POST")]
        public async Task ConfigTimeSeries()
        {
            using (var processor = new ShardedConfigurationHandlerProcessorForTimeSeriesConfig(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/timeseries/config", "GET")]
        public async Task GetTimeSeriesConfiguration()
        {
            using (var processor = new ShardedConfigurationHandlerProcessorForGetTimeSeriesConfiguration(this))
                await processor.ExecuteAsync();
        }
    }
}
