using System.Threading.Tasks;
using Raven.Server.Documents.ShardedHandlers.Processors;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;

namespace Raven.Server.Documents.ShardedHandlers
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
