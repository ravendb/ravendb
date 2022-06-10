using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.ETL;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public class ShardedOlapEtlHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/admin/etl/olap/test", "POST")]
    public async Task PostScriptTest()
    {
        using (var processor = new ShardedOlapEtlHandlerProcessorForTest(this))
            await processor.ExecuteAsync();
    }
}
