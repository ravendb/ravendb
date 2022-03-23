using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Documents.Sharding.Handlers.Processors.SampleData;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedSampleDataHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/studio/sample-data", "POST")]
        public async Task CreateSampleData()
        {
            using (var processor = new ShardedSampleDataHandlerProcessorForPostSampleData(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenShardedAction("/databases/*/studio/sample-data/classes", "GET")]
        public async Task GetSampleDataClasses()
        {
            using (var processor = new ShardedSampleDataHandlerProcessorForGetSampleDataClasses(this))
            {
                await processor.ExecuteAsync();
            }
        }
    }
}
