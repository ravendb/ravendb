using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.DataArchival;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedDataArchivalHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/data-archival/config", "GET")]
        public async Task GetDataArchivalConfig()
        {
            using (var processor = new ShardedDataArchivalHandlerProcessorForGet(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/data-archival/config", "POST")]
        public async Task ConfigDataArchival()
        {
            using (var processor = new ShardedDataArchivalHandlerProcessorForPost(this))
                await processor.ExecuteAsync();
        }
    }
}
