using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.ETL;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedSqlEtlHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/etl/sql/test", "POST")]
        public async Task PostScriptTest()
        {
            using (var processor = new ShardedSqlEtlHandlerProcessorForTestSqlEtl(this))
                await processor.ExecuteAsync();
        }
    }
}
