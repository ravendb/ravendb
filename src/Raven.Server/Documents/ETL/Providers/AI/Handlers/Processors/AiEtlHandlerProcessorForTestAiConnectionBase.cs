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
    public AiIntegrationHandlerProcessorForTestAiConnection([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var aiConnectorType = RequestHandler.GetEnumQueryString<AiConnectorType>("type");
        if (aiConnectorType == AiConnectorType.None)
            throw new ArgumentException($"AI connector type cannot be '{AiConnectorType.None}'");

        InMemoryLoggerProvider logger = null;
        try
        {
            string jsonConfigString;
            using (var streamReader = new StreamReader(HttpContext.Request.Body))
                jsonConfigString = await streamReader.ReadToEndAsync();

            var aiConnectionString = new AiConnectionString();

            switch (aiConnectorType)
            {
                case AiConnectorType.OpenAi:
                    var openAiSettings = JsonConvert.DeserializeObject<OpenAiSettings>(jsonConfigString);
                    aiConnectionString.OpenAiSettings = openAiSettings;
                    break;

                case AiConnectorType.AzureOpenAi:
                    var azureOpenAiSettings = JsonConvert.DeserializeObject<AzureOpenAiSettings>(jsonConfigString);
                    aiConnectionString.AzureOpenAiSettings = azureOpenAiSettings;
                    break;

                case AiConnectorType.Ollama:
                    var ollamaSettings = JsonConvert.DeserializeObject<OllamaSettings>(jsonConfigString);
                    aiConnectionString.OllamaSettings = ollamaSettings;
                    break;

                case AiConnectorType.Embedded:
                    var embeddedSettings = JsonConvert.DeserializeObject<EmbeddedSettings>(jsonConfigString);
                    aiConnectionString.EmbeddedSettings = embeddedSettings;
                    break;

                case AiConnectorType.Google:
                    var googleSettings = JsonConvert.DeserializeObject<GoogleSettings>(jsonConfigString);
                    aiConnectionString.GoogleSettings = googleSettings;
                    break;

                case AiConnectorType.HuggingFace:
                    var huggingFace = JsonConvert.DeserializeObject<HuggingFaceSettings>(jsonConfigString);
                    aiConnectionString.HuggingFaceSettings = huggingFace;
                    break;

                case AiConnectorType.MistralAi:
                    var mistralAiSettings = JsonConvert.DeserializeObject<MistralAiSettings>(jsonConfigString);
                    aiConnectionString.MistralAiSettings = mistralAiSettings;
                    break;

                default:
                    throw new ArgumentOutOfRangeException();
            }

            var aiEtlConfiguration = new EmbeddingsGenerationConfiguration { Connection = aiConnectionString };

            (ITextEmbeddingGenerationService service, logger) = AiHelper.CreateServicesForTest(aiEtlConfiguration);
            var embeddings = await AiHelper.GenerateEmbeddingsAsync(service, EmbeddingsHelper.ValuesListToVerifyConnection);

            if (embeddings.Count != EmbeddingsHelper.ValuesListToVerifyConnection.Count)
                throw new Exception($"Failed to generate embeddings for test values. Expected '{EmbeddingsHelper.ValuesListToVerifyConnection.Count}' result, but got '{embeddings.Count}'.");

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
