using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.ETL;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public sealed class ShardedAiEtlHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/admin/etl/ai/test", "POST")]
    public async Task PostScriptTest()
    {
        using (var processor = new ShardedAiEtlHandlerProcessorForPostScriptTest(this))
            await processor.ExecuteAsync();
    }
}
