using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.ETL;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public sealed class ShardedGenAiHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/admin/ai/gen-ai/test", "POST")]
    public async Task PostScriptTest()
    {
        using (var processor = new ShardedGenAiHandlerProcessorForPostScriptTest(this))
            await processor.ExecuteAsync();
    }
}
