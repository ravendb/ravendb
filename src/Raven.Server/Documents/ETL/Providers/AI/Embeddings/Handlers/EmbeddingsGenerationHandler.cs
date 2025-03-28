using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.ETL.Providers.AI.Embeddings.Handlers;

public sealed class EmbeddingsGenerationHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/admin/ai/embeddings/test", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task PostScriptTest()
    {
        using (var processor = new EmbeddingsGenerationHandlerProcessorForPostScriptTest(this))
            await processor.ExecuteAsync();
    }
}
