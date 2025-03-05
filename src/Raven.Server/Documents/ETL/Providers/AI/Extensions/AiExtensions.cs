using System;
using System.ClientModel;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Net.Http;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.Connectors.AzureOpenAI;
using Microsoft.SemanticKernel.Connectors.Google;
using Microsoft.SemanticKernel.Connectors.HuggingFace;
using Microsoft.SemanticKernel.Connectors.MistralAI;
using Microsoft.SemanticKernel.Connectors.Onnx;
using Microsoft.SemanticKernel.Connectors.OpenAI;
using Microsoft.SemanticKernel.Embeddings;
using OllamaSharp;
using OpenAI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.ServerWide;
using GoogleApiVersion = Raven.Client.Documents.Operations.AI.GoogleAIVersion;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;

#pragma warning disable SKEXP0001
#pragma warning disable SKEXP0010
#pragma warning disable SKEXP0070

namespace Raven.Server.Documents.ETL.Providers.AI.Extensions;

public static class AiExtensions
{
    private static readonly BertOnnxOptions BertOnnxDefaults = new();

    public static BertOnnxOptions ToBertOnnxOptions(this OnnxSettings settings)
    {

        return new BertOnnxOptions
        {
            CaseSensitive = settings.CaseSensitive ?? BertOnnxDefaults.CaseSensitive,
            MaximumTokens = settings.MaximumTokens ?? BertOnnxDefaults.MaximumTokens,
            ClsToken = settings.ClsToken ?? BertOnnxDefaults.ClsToken,
            UnknownToken = settings.UnknownToken ?? BertOnnxDefaults.UnknownToken,
            SepToken = settings.SepToken ?? BertOnnxDefaults.SepToken,
            PadToken = settings.PadToken ?? BertOnnxDefaults.PadToken,
            UnicodeNormalization = settings.UnicodeNormalization ?? BertOnnxDefaults.UnicodeNormalization,
            PoolingMode = settings.PoolingMode?.ToEmbeddingPoolingMode() ?? BertOnnxDefaults.PoolingMode,
            NormalizeEmbeddings = settings.NormalizeEmbeddings ?? BertOnnxDefaults.NormalizeEmbeddings
        };
    }

    public static EmbeddingPoolingMode ToEmbeddingPoolingMode(this OnnxEmbeddingPoolingMode poolingMode)
    {
        switch (poolingMode)
        {
            case OnnxEmbeddingPoolingMode.Max:
                return EmbeddingPoolingMode.Max;
            case OnnxEmbeddingPoolingMode.Mean:
                return EmbeddingPoolingMode.Mean;
            case OnnxEmbeddingPoolingMode.MeanSquareRootTokensLength:
                return EmbeddingPoolingMode.MeanSquareRootTokensLength;
            default:
                throw new ArgumentOutOfRangeException(nameof(poolingMode), poolingMode, null);
        }
    }

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

    [Experimental("SKEXP0070")]
    public static IKernelBuilder AddCustomBertOnnxTextEmbeddingGeneration(
        this IKernelBuilder builder,
        BertOnnxOptions options = null,
        int? dimensions = null,
        string serviceId = null)
    {
        builder.Services.AddKeyedSingleton<ITextEmbeddingGenerationService>(
            serviceId,
            GenerateEmbeddings.CreateTextEmbeddingGenerationService(options, dimensions));

        return builder;
    }

    public static void Configure(this IKernelBuilder kernelBuilder, AiConnectionString connectionString, bool withLogging)
    {
        var connectorType = connectionString.GetActiveProvider();
        ConfigureInternal(kernelBuilder, connectorType, connectionString, withLogging);
    }
    
    public static void Configure(this IKernelBuilder kernelBuilder, EmbeddingsGenerationConfiguration configuration, bool withLogging)
    {
        ConfigureInternal(kernelBuilder, configuration.AiConnectorType ,configuration.Connection, withLogging);
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

            case AiConnectorType.Onnx:
                var onnxSettings = connectionString.OnnxSettings;
                kernelBuilder.AddCustomBertOnnxTextEmbeddingGeneration(onnxSettings.ToBertOnnxOptions(), onnxSettings.Dimensions);
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
                configure.SetMinimumLevel(LogLevel.Trace);
                configure.AddProvider(new InMemoryLoggerProvider());
            });
    }
}
