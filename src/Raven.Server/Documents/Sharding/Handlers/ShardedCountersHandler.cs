using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Counters;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedCountersHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/counters", "GET")]
        public async Task Get()
        {
            using (var processor = new ShardedCountersHandlerProcessorForGetCounters(this))
            {
                await processor.ExecuteAsync();
            }
        }
    }
}
