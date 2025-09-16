using System;
using System.ClientModel;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.SemanticKernel;
using OllamaSharp;
using OpenAI;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.ServerWide;
using GoogleApiVersion = Raven.Client.Documents.Operations.AI.GoogleAIVersion;
using VertexApiVersion = Raven.Client.Documents.Operations.AI.VertexAIVersion;

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
    
    public static Microsoft.SemanticKernel.Connectors.Google.VertexAIVersion ToVertexApiVersion(this VertexApiVersion vertexApiVersion)
    {
        switch (vertexApiVersion)
        {
            case VertexApiVersion.V1:
                return Microsoft.SemanticKernel.Connectors.Google.VertexAIVersion.V1;
            case VertexApiVersion.V1_Beta:
                return Microsoft.SemanticKernel.Connectors.Google.VertexAIVersion.V1_Beta;
            default:
                throw new ArgumentOutOfRangeException(nameof(vertexApiVersion), vertexApiVersion, null);
        }
    }

    public static IKernelBuilder AddCustomBertOnnxTextEmbeddingGeneration(this IKernelBuilder builder, string serviceId = null)
    {
        builder.Services.AddKeyedSingleton(serviceId, GenerateEmbeddings.Embedder.Value);
        return builder;
    }


    public static void Configure(this IKernelBuilder kernelBuilder, AiConnectionString connectionString, bool withLogging)
    {
        var connectorType = connectionString.GetActiveProvider();
        ConfigureInternal(kernelBuilder, connectorType, connectionString, withLogging);
    }
    
    public static void Configure<TConfig>(this IKernelBuilder kernelBuilder, TConfig configuration, bool withLogging)
        where TConfig : AbstractAiIntegrationConfiguration
    {
        ConfigureInternal(kernelBuilder, configuration.AiConnectorType, configuration.Connection, withLogging);
    }

    private static void ConfigureInternal(this IKernelBuilder kernelBuilder, AiConnectorType connectorType, AiConnectionString connectionString, bool withLogging)
    {
        List<string> errors = [];
        if (connectionString.Validate(errors) == false)
            throw new InvalidOperationException($"Connection string is invalid due to the following errors:{Environment.NewLine}" +
                                                $" - {string.Join($"{Environment.NewLine} - ", errors)}");
        
        switch (connectorType)
        {
            case AiConnectorType.OpenAi:
                var openAiSettings = connectionString.OpenAiSettings;

                var apiKey = new ApiKeyCredential(openAiSettings.ApiKey);
                var openAiOptions = new OpenAIClientOptions
                {
                    Endpoint = openAiSettings.GetBaseEndpointUri(),
                    ProjectId = openAiSettings.ProjectId,
                    UserAgentApplicationId = $"RavenDB-{ServerVersion.Version}"
                };
                var openAIClient = new OpenAIClient(apiKey, openAiOptions);

                kernelBuilder.AddOpenAIEmbeddingGenerator(openAiSettings.Model, openAIClient, dimensions: openAiSettings.Dimensions);
                break;

            case AiConnectorType.AzureOpenAi:
                var azureOpenAiSettings = connectionString.AzureOpenAiSettings;

                kernelBuilder.AddAzureOpenAIEmbeddingGenerator(
                    azureOpenAiSettings.DeploymentName,
                    azureOpenAiSettings.GetBaseEndpointUri().ToString(),
                    azureOpenAiSettings.ApiKey,
                    modelId: azureOpenAiSettings.Model,
                    dimensions: azureOpenAiSettings.Dimensions);
                break;

            case AiConnectorType.Ollama:
                var ollamaSettings = connectionString.OllamaSettings;
                var ollamaApiConfig = new OllamaApiClient.Configuration { Uri = ollamaSettings.GetBaseEndpointUri(), Model = ollamaSettings.Model };
                
                var ollamaApiClient = new OllamaApiClient(ollamaApiConfig);

                kernelBuilder.AddOllamaEmbeddingGenerator(ollamaApiClient);
                break;

            case AiConnectorType.Embedded:
                kernelBuilder.AddCustomBertOnnxTextEmbeddingGeneration();
                break;

            case AiConnectorType.Google:
                var googleSettings = connectionString.GoogleSettings;

                if (googleSettings.AiVersion.HasValue)
                {
                    kernelBuilder.AddGoogleAIEmbeddingGenerator(
                        googleSettings.Model,
                        googleSettings.ApiKey,
                        googleSettings.AiVersion.Value.ToGoogleApiVersion(),
                        dimensions: googleSettings.Dimensions);
                }
                else
                {
                    kernelBuilder.AddGoogleAIEmbeddingGenerator(
                        googleSettings.Model,
                        googleSettings.ApiKey,
                        dimensions: googleSettings.Dimensions);
                }

                break;
            
            case AiConnectorType.Vertex:
                var vertexSettings = connectionString.VertexSettings;

                if (vertexSettings.AiVersion.HasValue)
                {
                    kernelBuilder.AddVertexAIEmbeddingGenerator(
                        vertexSettings.Model,
                        vertexSettings.ApiKey,
                        vertexSettings.Location,
                        vertexSettings.ProjectId,
                        vertexSettings.AiVersion.Value.ToVertexApiVersion());
                }
                else
                {
                    kernelBuilder.AddVertexAIEmbeddingGenerator(
                        vertexSettings.Model,
                        vertexSettings.ApiKey,
                        vertexSettings.Location,
                        vertexSettings.ProjectId);
                }

                break;

            case AiConnectorType.HuggingFace:
                var huggingFaceSettings = connectionString.HuggingFaceSettings;
                var huggingFaceUri = string.IsNullOrWhiteSpace(huggingFaceSettings.Endpoint) ? null : new Uri(huggingFaceSettings.Endpoint);

                  kernelBuilder.AddHuggingFaceEmbeddingGenerator(
                    huggingFaceSettings.Model,
                    huggingFaceUri,
                    huggingFaceSettings.ApiKey);
                break;

            case AiConnectorType.MistralAi:
                var mistralSettings = connectionString.MistralAiSettings;
                var mistralUri = new Uri(mistralSettings.Endpoint);

                kernelBuilder.AddMistralEmbeddingGenerator(
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
