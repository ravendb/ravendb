using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.ETL;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public sealed class ShardedAiIntegrationHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/admin/ai/embeddings/test", "POST")]
    public async Task PostScriptTest()
    {
        using (var processor = new ShardedEmbeddingsGenerationHandlerProcessorForPostScriptTest(this))
            await processor.ExecuteAsync();
    }
}
