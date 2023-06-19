using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Archival;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedArchivalHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/archival/config", "GET")]
        public async Task GetArchivalConfig()
        {
            using (var processor = new ShardedArchivalHandlerProcessorForGet(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/archival/config", "POST")]
        public async Task ConfigArchival()
        {
            using (var processor = new ShardedArchivalHandlerProcessorForPost(this))
                await processor.ExecuteAsync();
        }
    }
}
