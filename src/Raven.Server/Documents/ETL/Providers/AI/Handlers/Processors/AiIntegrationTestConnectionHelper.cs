using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Json;
using Raven.Server.Web;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

#pragma warning disable SKEXP0001

namespace Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;

internal static class AiIntegrationTestConnectionHelper
{
    public static async Task ExecuteAsync(RequestHandler requestHandler, CancellationToken cancellationToken)
    {
        var aiConnectorType = requestHandler.GetEnumQueryString<AiConnectorType>("type");
        if (aiConnectorType == AiConnectorType.None)
            throw new ArgumentException($"AI connector type cannot be '{AiConnectorType.None}'");

        var modelType = requestHandler.GetEnumQueryString<AiModelType>("modelType");

        InMemoryLoggerProvider logger = null;
        try
        {
            using (requestHandler.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(requestHandler.RequestBodyStream(), "etl/test/script");

                var aiConnectionString = new AiConnectionString { ModelType = modelType };

                switch (aiConnectorType)
                {
                    case AiConnectorType.OpenAi:
                        var openAiSettings = JsonDeserializationServer.OpenAiSettings(json);
                        aiConnectionString.OpenAiSettings = openAiSettings;
                        break;

                    case AiConnectorType.AzureOpenAi:
                        var azureOpenAiSettings = JsonDeserializationServer.AzureOpenAiSettings(json);
                        aiConnectionString.AzureOpenAiSettings = azureOpenAiSettings;
                        break;

                    case AiConnectorType.Ollama:
                        var ollamaSettings = JsonDeserializationServer.OllamaSettings(json);
                        aiConnectionString.OllamaSettings = ollamaSettings;
                        break;

                    case AiConnectorType.Embedded:
                        var embeddedSettings = JsonDeserializationServer.EmbeddedSettings(json);
                        aiConnectionString.EmbeddedSettings = embeddedSettings;
                        break;

                    case AiConnectorType.Google:
                        var googleSettings = JsonDeserializationServer.GoogleSettings(json);
                        aiConnectionString.GoogleSettings = googleSettings;
                        break;

                    case AiConnectorType.Vertex:
                        var vertexSettings = JsonDeserializationServer.VertexSettings(json);
                        aiConnectionString.VertexSettings = vertexSettings;
                        break;

                    case AiConnectorType.HuggingFace:
                        var huggingFace = JsonDeserializationServer.HuggingFaceSettings(json);
                        aiConnectionString.HuggingFaceSettings = huggingFace;
                        break;

                    case AiConnectorType.MistralAi:
                        var mistralAiSettings = JsonDeserializationServer.MistralAiSettings(json);
                        aiConnectionString.MistralAiSettings = mistralAiSettings;
                        break;

                    default:
                        throw new ArgumentOutOfRangeException();
                }

                switch (aiConnectionString.ModelType)
                {
                    case AiModelType.TextEmbeddings:
                        var aiEtlConfiguration = new EmbeddingsGenerationConfiguration { Connection = aiConnectionString };
                        (IEmbeddingGenerator<string, Embedding<float>> service, logger) = AiHelper.CreateEmbeddingServicesForTest(aiEtlConfiguration);
                        var embeddings = await service.GenerateAsync(EmbeddingsHelper.ValuesListToVerifyConnection, cancellationToken: cancellationToken);

                        if (embeddings.Count != EmbeddingsHelper.ValuesListToVerifyConnection.Count)
                            throw new EmbeddingsMismatchException(
                                $"Failed to generate embeddings for test values. Expected '{EmbeddingsHelper.ValuesListToVerifyConnection.Count}' result, but got '{embeddings.Count}'.");
                        break;
                    case AiModelType.Chat:
                        using (var client = ChatCompletionClient.CreateChatCompletionClient(requestHandler.ServerStore.ContextPool, aiConnectionString))
                        {
                            var schema = ChatCompletionClient.GetSchemaFromSampleObject("{\"answer\":\"the answer to the user's prompt\"}");
                            await client.TestCompleteAsync("Reply with exact word only: raven", "hi", schema, requestHandler.HttpContext.RequestAborted);
                        }

                        break;
                    default:
                        throw new ArgumentOutOfRangeException("Invalid model type: " + aiConnectionString.ModelType);
                }

                var result = new DynamicJsonValue { [nameof(NodeConnectionTestResult.Success)] = true };

                await using (var writer = new AsyncBlittableJsonTextWriter(context, requestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, result);
                }
            }
        }
        catch (Exception e)
        {
            var result = new DynamicJsonValue
            {
                [nameof(NodeConnectionTestResult.Success)] = false,
                [nameof(NodeConnectionTestResult.Error)] = e.ToString()
            };

            if (logger != null)
            {
                var logsArray = new DynamicJsonArray(collection: logger.GetLogs());
                result[nameof(NodeConnectionTestResult.Log)] = logsArray;
            }

            using (requestHandler.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, requestHandler.ResponseBodyStream()))
            {
                context.Write(writer, result);
            }
        }
        finally
        {
            logger?.Dispose();
        }
    }
}
