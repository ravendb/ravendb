using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors.Studio;
using Raven.Server.Routing;

namespace Raven.Server.Web.Studio;

public class StudioQueryingAssistantHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/studio/tasks/embeddings", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetEmbeddingGenerationTasks()
    {
        using (var processor = new StudioQueryingAssistantProcessorForEmbeddingsGenerationTasks(this))
            await processor.ExecuteAsync();
    }
}
