using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    internal class ShardedAdminConfigurationHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/configuration/studio", "PUT")]
        public async Task GetStudioConfiguration()
        {
            using (var processor = new ShardedConfigurationHandlerProcessorForPutAdminConfiguration(this))
            {
                await processor.ExecuteAsync();
            }
        }
    }
}
