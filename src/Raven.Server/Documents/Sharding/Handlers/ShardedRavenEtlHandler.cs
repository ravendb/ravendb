using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.ETL;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedRavenEtlHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/etl/raven/test", "POST")]
        public async Task PostScriptTest()
        {
            using (var processor = new ShardedRavenEtlHandlerProcessorForTestEtl(this))
                await processor.ExecuteAsync();
        }
    }
}
