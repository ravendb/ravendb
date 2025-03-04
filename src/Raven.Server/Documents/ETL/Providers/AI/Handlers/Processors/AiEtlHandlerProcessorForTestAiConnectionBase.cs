using System;
using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.SemanticKernel.Embeddings;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;
#pragma warning disable SKEXP0001

namespace Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;

internal class AiIntegrationHandlerProcessorForTestAiConnection<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    private protected string JsonConfigString;
    public AiConnectorType AiConnectorType { get; init; }

    public AiIntegrationHandlerProcessorForTestAiConnection([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        InMemoryLoggerProvider logger = null;
        try
        {
            using (var streamReader = new StreamReader(HttpContext.Request.Body))
                JsonConfigString = await streamReader.ReadToEndAsync();

            var aiConnectionString = new AiConnectionString();

            switch (AiConnectorType)
            {
                case AiConnectorType.OpenAi:
                    var openAiSettings = JsonConvert.DeserializeObject<OpenAiSettings>(JsonConfigString);
                    aiConnectionString.OpenAiSettings = openAiSettings;
                    break;

                case AiConnectorType.AzureOpenAi:
                    var azureOpenAiSettings = JsonConvert.DeserializeObject<AzureOpenAiSettings>(JsonConfigString);
                    aiConnectionString.AzureOpenAiSettings = azureOpenAiSettings;
                    break;

                case AiConnectorType.Ollama:
                    var ollamaSettings = JsonConvert.DeserializeObject<OllamaSettings>(JsonConfigString);
                    aiConnectionString.OllamaSettings = ollamaSettings;
                    break;

                case AiConnectorType.Onnx:
                    var onnxSettings = JsonConvert.DeserializeObject<OnnxSettings>(JsonConfigString);
                    aiConnectionString.OnnxSettings = onnxSettings;
                    break;

                case AiConnectorType.Google:
                    var googleSettings = JsonConvert.DeserializeObject<GoogleSettings>(JsonConfigString);
                    aiConnectionString.GoogleSettings = googleSettings;
                    break;

                case AiConnectorType.HuggingFace:
                    var huggingFace = JsonConvert.DeserializeObject<HuggingFaceSettings>(JsonConfigString);
                    aiConnectionString.HuggingFaceSettings = huggingFace;
                    break;

                case AiConnectorType.MistralAi:
                    var mistralAiSettings = JsonConvert.DeserializeObject<MistralAiSettings>(JsonConfigString);
                    aiConnectionString.MistralAiSettings = mistralAiSettings;
                    break;

                default:
                    throw new ArgumentOutOfRangeException();
            }

            var aiEtlConfiguration = new EmbeddingsGenerationConfiguration { Connection = aiConnectionString };

            (ITextEmbeddingGenerationService service, logger) = AiHelper.CreateServicesForTest(aiEtlConfiguration);
            var embeddings = await service.GenerateEmbeddingsAsync(EmbeddingsHelper.TestValuesList);

            if (embeddings.Count != EmbeddingsHelper.TestValuesList.Count)
                throw new Exception($"Failed to generate embeddings for test values. Expected '{EmbeddingsHelper.TestValuesList.Count}' result, but got '{embeddings.Count}'.");

            var result = new DynamicJsonValue { [nameof(NodeConnectionTestResult.Success)] = true };

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, result);
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

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
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
