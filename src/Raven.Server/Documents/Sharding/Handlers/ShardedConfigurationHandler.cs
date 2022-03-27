using System.Threading.Tasks;
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

        [RavenShardedAction("/databases/*/configuration/client", "GET")]
        public async Task GetClientConfiguration()
        {
            using (var processor = new ShardedConfigurationHandlerProcessorForGetClientConfiguration(this))
                await processor.ExecuteAsync();
        }
    }
}
