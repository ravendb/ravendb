using System;
using System.ClientModel;
using System.Data.HashFunction;
using System.Data.HashFunction.Blake2;
using System.Diagnostics.CodeAnalysis;
using Microsoft.SemanticKernel.Embeddings;
using Microsoft.SemanticKernel;
using OllamaSharp;
using OpenAI;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Server.Documents.ETL.Providers.AI.Extensions;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.ETL.Providers.AI;

public static class AiHelper
{
    private static readonly IBlake2B Hash;

    static AiHelper()
    {
        Hash = Blake2BFactory.Instance.Create(new Blake2BConfig
        {
            HashSizeInBits = 256
        });
    }

    public static string CalculateValueHash(string value)
    {
        return Hash.ComputeHash(value).AsHexString(uppercase: true);
    }

    public static string GetDocumentEmbeddingsId(string documentId)
    {
        return $"{documentId}/embeddings";
    }

    public static string GetValueEmbeddingsDocumentId(string configurationName, string hash)
    {
        return $"embeddings/{configurationName}/{hash}";
    }

    [Experimental("SKEXP0001")]
    public static ITextEmbeddingGenerationService CreateService(AiEtlConfiguration configuration)
    {
        var kernelBuilder = Kernel.CreateBuilder();

        switch (configuration.AiConnectorType)
        {
            case AiConnectorType.OpenAi:
                var openAiSettings = configuration.Connection.OpenAiSettings;

                var apiKey = new ApiKeyCredential(openAiSettings.ApiKey);
                var openAiOptions = new OpenAIClientOptions
                {
                    Endpoint = new Uri(openAiSettings.Endpoint),
                    ProjectId = openAiSettings.ProjectId,
                    UserAgentApplicationId = $"RavenDB/{ServerVersion.Version}/{nameof(AiEtl)}"
                };
                var openAIClient = new OpenAIClient(apiKey, openAiOptions);
                kernelBuilder.AddOpenAITextEmbeddingGeneration(openAiSettings.Model, openAIClient);

                break;

            case AiConnectorType.AzureOpenAI:
                var azureOpenAiSettings = configuration.Connection.AzureOpenAiSettings;

                kernelBuilder.AddAzureOpenAITextEmbeddingGeneration(
                    azureOpenAiSettings.DeploymentName,
                    azureOpenAiSettings.Endpoint,
                    azureOpenAiSettings.ApiKey,
                    azureOpenAiSettings.ServiceId,
                    azureOpenAiSettings.Model);

                break;

            case AiConnectorType.Ollama:
                var ollamaSettings = configuration.Connection.OllamaSettings;
                var ollamaApiConfig = new OllamaApiClient.Configuration
                {
                    Uri = new Uri(ollamaSettings.Uri),
                    Model = ollamaSettings.Model
                };

                var ollamaApiClient = new OllamaApiClient(ollamaApiConfig);

                kernelBuilder.AddOllamaTextEmbeddingGeneration(ollamaApiClient);

                // var modelInfo = AsyncHelpers.RunSync(() => ollamaApiClient.ShowModelAsync(ollamaSettings.Model));

                break;

            case AiConnectorType.Onnx:
                var onnxSettings = configuration.Connection.OnnxSettings;
                kernelBuilder.AddCustomBertOnnxTextEmbeddingGeneration(onnxSettings.ToBertOnnxOptions());
                break;

            case AiConnectorType.Google:
                var googleSettings = configuration.Connection.GoogleSettings;

                if (googleSettings.AiVersion.HasValue)
                    kernelBuilder.AddGoogleAIEmbeddingGeneration(googleSettings.Model, googleSettings.ApiKey, googleSettings.AiVersion.Value.ToGoogleAiVersion());
                else
                    kernelBuilder.AddGoogleAIEmbeddingGeneration(googleSettings.Model, googleSettings.ApiKey);

                break;

            case AiConnectorType.HuggingFace:
                var huggingFaceSettings = configuration.Connection.HuggingFaceSettings;
                var uri = string.IsNullOrWhiteSpace(huggingFaceSettings.Endpoint) ? null : new Uri(huggingFaceSettings.Endpoint);

                kernelBuilder.AddHuggingFaceTextEmbeddingGeneration(huggingFaceSettings.Model, uri, huggingFaceSettings.ApiKey);

                break;

            default:
                throw new NotSupportedException($"'{configuration.AiConnectorType}' provider is not supported");
        }

        var kernel = kernelBuilder.Build();
        return kernel.GetRequiredService<ITextEmbeddingGenerationService>();
    }
}
