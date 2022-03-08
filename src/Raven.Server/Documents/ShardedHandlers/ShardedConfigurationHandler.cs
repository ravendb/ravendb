using System.Threading.Tasks;
using Raven.Server.Documents.ShardedHandlers.Processors;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedConfigurationHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/configuration/studio", "GET")]
        public async Task GetStudioConfiguration()
        {
            using (var processor = new ShardedConfigurationHandlerProcessorForGetStudioConfiguration(this, ShardedContext))
                await processor.ExecuteAsync();
        }
    }
}
