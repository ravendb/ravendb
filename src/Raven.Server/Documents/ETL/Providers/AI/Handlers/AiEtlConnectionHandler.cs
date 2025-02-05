using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.AI.Handlers;

public sealed class AiEtlConnectionHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/admin/etl/ai/azureopenai/test-connection", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task TestAzureOpenAiConnection()
    {
        using (var processor = new AiEtlHandlerProcessorForTestAzureOpenAiConnection<DatabaseRequestHandler, DocumentsOperationContext>(this))
            await processor.ExecuteAsync();
    }

    [RavenAction("/databases/*/admin/etl/ai/google/test-connection", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task TestGoogleConnection()
    {
        using (var processor = new AiEtlHandlerProcessorForTestGoogleConnection<DatabaseRequestHandler, DocumentsOperationContext>(this))
            await processor.ExecuteAsync();
    }

    [RavenAction("/databases/*/admin/etl/ai/huggingface/test-connection", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task TestHuggingFaceConnection()
    {
        using (var processor = new AiEtlHandlerProcessorForTestHuggingFaceConnection<DatabaseRequestHandler, DocumentsOperationContext>(this))
            await processor.ExecuteAsync();
    }

    [RavenAction("/databases/*/admin/etl/ai/ollama/test-connection", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task TestOllamaConnection()
    {
        using (var processor = new AiEtlHandlerProcessorForTestOllamaConnection<DatabaseRequestHandler, DocumentsOperationContext>(this))
            await processor.ExecuteAsync();
    }

    [RavenAction("/databases/*/admin/etl/ai/onnx/test-connection", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task TestOnnxConnection()
    {
        using (var processor = new AiEtlHandlerProcessorForTestOnnxConnection<DatabaseRequestHandler, DocumentsOperationContext>(this))
            await processor.ExecuteAsync();
    }

    [RavenAction("/databases/*/admin/etl/ai/openai/test-connection", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task TestOpenAiConnection()
    {
        using (var processor = new AiEtlHandlerProcessorForTestOpenAiConnection<DatabaseRequestHandler, DocumentsOperationContext>(this))
            await processor.ExecuteAsync();
    }
}
