using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.ETL;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public class ShardedElasticSearchEtlHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/admin/etl/elasticsearch/test", "POST")]
    public async Task Test()
    {
        using (var processor = new ShardedElasticSearchEtlHandlerProcessorForTest(this))
            await processor.ExecuteAsync();
    }
}
