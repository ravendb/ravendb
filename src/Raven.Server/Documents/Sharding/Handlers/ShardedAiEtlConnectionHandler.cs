using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.AI.Handlers;

public sealed class ShardedAiEtlConnectionHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/admin/etl/ai/azureopenai/test-connection", "POST")]
    public async Task TestAzureOpenAiConnection()
    {
        using (var processor = new AiEtlHandlerProcessorForTestAzureOpenAiConnection<ShardedDatabaseRequestHandler, TransactionOperationContext>(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/admin/etl/ai/google/test-connection", "POST")]
    public async Task TestGoogleConnection()
    {
        using (var processor = new AiEtlHandlerProcessorForTestGoogleConnection<ShardedDatabaseRequestHandler, TransactionOperationContext>(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/admin/etl/ai/huggingface/test-connection", "POST")]
    public async Task TestHuggingFaceConnection()
    {
        using (var processor = new AiEtlHandlerProcessorForTestHuggingFaceConnection<ShardedDatabaseRequestHandler, TransactionOperationContext>(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/admin/etl/ai/ollama/test-connection", "POST")]
    public async Task TestOllamaConnection()
    {
        using (var processor = new AiEtlHandlerProcessorForTestOllamaConnection<ShardedDatabaseRequestHandler, TransactionOperationContext>(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/admin/etl/ai/onnx/test-connection", "POST")]
    public async Task TestOnnxConnection()
    {
        using (var processor = new AiEtlHandlerProcessorForTestOnnxConnection<ShardedDatabaseRequestHandler, TransactionOperationContext>(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/admin/etl/ai/openai/test-connection", "POST")]
    public async Task TestOpenAiConnection()
    {
        using (var processor = new AiEtlHandlerProcessorForTestOpenAiConnection<ShardedDatabaseRequestHandler, TransactionOperationContext>(this))
            await processor.ExecuteAsync();
    }
}
