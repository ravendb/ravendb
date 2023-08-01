using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.ETL;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public sealed class ShardedQueueEtlHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/etl/queue/test", "POST")]
        public async Task PostScriptTest()
        {
            using (var processor = new ShardedQueueEtlHandlerProcessorForPostScriptTest(this))
                await processor.ExecuteAsync();
        }
    }
}
