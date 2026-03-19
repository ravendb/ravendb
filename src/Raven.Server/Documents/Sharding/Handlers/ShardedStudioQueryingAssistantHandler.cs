using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Studio;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers;

public class ShardedStudioQueryingAssistantHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/studio/tasks/embeddings", "GET")]
    public async Task GetEmbeddingGenerationTasks()
    {
        using (var processor = new ShardedStudioQueryingAssistantProcessorForEmbeddingsGenerationTasks(this))
            await processor.ExecuteAsync();
    }
}
