using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Documents.Sharding.Handlers.Processors.Configuration;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedConfigurationHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/configuration/studio", "GET")]
        public async Task GetStudioConfiguration()
        {
            using (var processor = new ShardedConfigurationHandlerProcessorForGetStudioConfiguration(this))
                await processor.ExecuteAsync();
        }
    }
}
