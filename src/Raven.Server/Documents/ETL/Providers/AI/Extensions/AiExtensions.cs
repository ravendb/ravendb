using System;
using System.ClientModel;
using System.Collections.Generic;
using System.Net.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.Embeddings;
using OllamaSharp;
using OpenAI;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.ServerWide;
using GoogleApiVersion = Raven.Client.Documents.Operations.AI.GoogleAIVersion;

#pragma warning disable SKEXP0001
#pragma warning disable SKEXP0010
#pragma warning disable SKEXP0070

namespace Raven.Server.Documents.ETL.Providers.AI.Extensions;

public static class AiExtensions
{
    public static Microsoft.SemanticKernel.Connectors.Google.GoogleAIVersion ToGoogleApiVersion(this GoogleApiVersion googleApiVersion)
    {
        switch (googleApiVersion)
        {
            case GoogleApiVersion.V1:
                return Microsoft.SemanticKernel.Connectors.Google.GoogleAIVersion.V1;
            case GoogleApiVersion.V1_Beta:
                return Microsoft.SemanticKernel.Connectors.Google.GoogleAIVersion.V1_Beta;
            default:
                throw new ArgumentOutOfRangeException(nameof(googleApiVersion), googleApiVersion, null);
        }
    }

    public static IKernelBuilder AddCustomBertOnnxTextEmbeddingGeneration(this IKernelBuilder builder, string serviceId = null)
    {
        builder.Services.AddKeyedSingleton<ITextEmbeddingGenerationService>(serviceId, GenerateEmbeddings.Embedder.Value);
        return builder;
    }

    public static void Configure(this IKernelBuilder kernelBuilder, AiConnectionString connectionString, bool withLogging)
    {
        var connectorType = connectionString.GetActiveProvider();
        ConfigureInternal(kernelBuilder, connectorType, connectionString, withLogging);
    }
    
    public static void Configure(this IKernelBuilder kernelBuilder, EmbeddingsGenerationConfiguration configuration, bool withLogging)
    {
        ConfigureInternal(kernelBuilder, configuration.AiConnectorType, configuration.Connection, withLogging);
    }

    private static void ConfigureInternal(this IKernelBuilder kernelBuilder, AiConnectorType connectorType, AiConnectionString connectionString, bool withLogging)
    {
        var errors = new List<string>();
        if (connectionString.Validate(ref errors) == false)
            throw new InvalidOperationException($"Connection string is invalid due to the following errors:{Environment.NewLine}" +
                                                $" - {string.Join($"{Environment.NewLine} - ", errors)}");
        
        switch (connectorType)
        {
            case AiConnectorType.OpenAi:
                var openAiSettings = connectionString.OpenAiSettings;

                var apiKey = new ApiKeyCredential(openAiSettings.ApiKey);
                var openAiOptions = new OpenAIClientOptions
                {
                    Endpoint = new Uri(openAiSettings.Endpoint),
                    ProjectId = openAiSettings.ProjectId,
                    UserAgentApplicationId = $"RavenDB-{ServerVersion.Version}"
                };
                var openAIClient = new OpenAIClient(apiKey, openAiOptions);

                kernelBuilder.AddOpenAITextEmbeddingGeneration(openAiSettings.Model, openAIClient, dimensions: openAiSettings.Dimensions);
                break;

            case AiConnectorType.AzureOpenAi:
                var azureOpenAiSettings = connectionString.AzureOpenAiSettings;

                kernelBuilder.AddAzureOpenAITextEmbeddingGeneration(
                    azureOpenAiSettings.DeploymentName,
                    azureOpenAiSettings.Endpoint,
                    azureOpenAiSettings.ApiKey,
                    modelId: azureOpenAiSettings.Model,
                    dimensions: azureOpenAiSettings.Dimensions);
                break;

            case AiConnectorType.Ollama:
                var ollamaSettings = connectionString.OllamaSettings;
                var ollamaApiConfig = new OllamaApiClient.Configuration { Uri = new Uri(ollamaSettings.Uri), Model = ollamaSettings.Model };

                var ollamaApiClient = new OllamaApiClient(ollamaApiConfig);

                kernelBuilder.AddOllamaTextEmbeddingGeneration(ollamaApiClient);
                break;

            case AiConnectorType.Embedded:
                kernelBuilder.AddCustomBertOnnxTextEmbeddingGeneration();
                break;

            case AiConnectorType.Google:
                var googleSettings = connectionString.GoogleSettings;

                HttpClient httpClient = null;
                if (googleSettings.Dimensions.HasValue)
                    httpClient = HttpClientExtensions.CreateWithDimensionality(googleSettings.Dimensions.Value);

                if (googleSettings.AiVersion.HasValue)
                    kernelBuilder.AddGoogleAIEmbeddingGeneration(
                        googleSettings.Model,
                        googleSettings.ApiKey,
                        googleSettings.AiVersion.Value.ToGoogleApiVersion(),
                        httpClient: httpClient);
                else
                    kernelBuilder.AddGoogleAIEmbeddingGeneration(
                        googleSettings.Model,
                        googleSettings.ApiKey,
                        httpClient: httpClient);
                break;

            case AiConnectorType.HuggingFace:
                var huggingFaceSettings = connectionString.HuggingFaceSettings;
                var huggingFaceUri = string.IsNullOrWhiteSpace(huggingFaceSettings.Endpoint) ? null : new Uri(huggingFaceSettings.Endpoint);

                  kernelBuilder.AddHuggingFaceTextEmbeddingGeneration(
                    huggingFaceSettings.Model,
                    huggingFaceUri,
                    huggingFaceSettings.ApiKey);
                break;

            case AiConnectorType.MistralAi:
                var mistralSettings = connectionString.MistralAiSettings;
                var mistralUri = new Uri(mistralSettings.Endpoint);

                kernelBuilder.AddMistralTextEmbeddingGeneration(
                    mistralSettings.Model,
                    mistralSettings.ApiKey,
                    mistralUri);
                break;

            default:
                throw new NotSupportedException($"'{connectorType}' provider is not supported");
        }
        
        if (withLogging)
            kernelBuilder.Services.AddLogging(configure =>
            {
                configure.SetMinimumLevel(LogLevel.Debug);
                configure.AddProvider(new InMemoryLoggerProvider());
            });
    }
}
