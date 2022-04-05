using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Configuration;
using Raven.Server.Documents.Sharding.Handlers.Processors.TimeSeries;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedTimeSeriesHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/timeseries", "POST")]
        public async Task Batch()
        {
            using (var processor = new ShardedTimeSeriesHandlerProcessorForPostTimeSeries(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/timeseries", "GET")]
        public async Task Read()
        {
            using (var processor = new ShardedTimeSeriesHandlerProcessorForGetTimeSeries(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/timeseries/config", "POST")]
        public async Task PostTimeSeriesConfiguration()
        {
            using (var processor = new ShardedConfigurationHandlerProcessorForPostTimeSeriesConfiguration(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/timeseries/config", "GET")]
        public async Task GetTimeSeriesConfiguration()
        {
            using (var processor = new ShardedConfigurationHandlerProcessorForGetTimeSeriesConfiguration(this))
                await processor.ExecuteAsync();
        }
        
        [RavenShardedAction("/databases/*/timeseries/stats", "GET")]
        public async Task Stats()
        {
            using (var processor = new ShardedTimeSeriesHandlerProcessorForGetTimeSeriesStats(this))
                await processor.ExecuteAsync();
        }
    }
}
