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

    public static IKernelBuilder AddTransientCustomBertOnnxTextEmbeddingGeneration(
        this IKernelBuilder builder,
        BertOnnxOptions options = null,
        int? dimensions = null,
        string serviceId = null)
    {
        builder.Services.AddKeyedTransient<ITextEmbeddingGenerationService>(
            serviceId,
            (_, _) => GenerateEmbeddings.CreateTextEmbeddingGenerationService(options, dimensions));

        return builder;
    }

    public static IKernelBuilder AddTransientOpenAiEmbeddingGeneration(
        this IKernelBuilder builder,
        string modelId,
        OpenAIClient openAIClient,
        string serviceId = null,
        int? dimensions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(openAIClient);
        ArgumentException.ThrowIfNullOrEmpty(modelId);

        builder.Services.AddKeyedTransient<ITextEmbeddingGenerationService>(
            serviceId,
            (serviceProvider, _) => new OpenAITextEmbeddingGenerationService(
                modelId,
                openAIClient,
                serviceProvider.GetService<ILoggerFactory>(),
                dimensions));

        return builder;
    }

    public static IKernelBuilder AddTransientAzureOpenAiEmbeddingGeneration(
        this IKernelBuilder builder,
        string deploymentName,
        string endpoint,
        string apiKey,
        string serviceId = null,
        string modelId = null,
        HttpClient httpClient = null,
        int? dimensions = null,
        string apiVersion = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(deploymentName);
        ArgumentException.ThrowIfNullOrEmpty(endpoint);
        ArgumentException.ThrowIfNullOrEmpty(apiKey);
        ArgumentException.ThrowIfNullOrEmpty(modelId);


        builder.Services.AddKeyedTransient<ITextEmbeddingGenerationService>(serviceId, (serviceProvider, _) =>
            new AzureOpenAITextEmbeddingGenerationService(
                deploymentName,
                endpoint,
                apiKey,
                modelId,
                httpClient ?? serviceProvider.GetService<HttpClient>(),
                serviceProvider.GetService<ILoggerFactory>(),
                dimensions,
                apiVersion));

        return builder;
    }

    public static IKernelBuilder AddTransientOllamaEmbeddingGeneration(
        this IKernelBuilder builder,
        OllamaApiClient ollamaClient,
        string serviceId = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(ollamaClient);

        builder.Services.AddKeyedTransient(serviceId, (serviceProvider, _) =>
        {
            var loggerFactory = serviceProvider.GetService<ILoggerFactory>();
            var embeddingGeneratorBuilder = ((IEmbeddingGenerator<string, Embedding<float>>)ollamaClient).AsBuilder();

            if (loggerFactory is not null)
                embeddingGeneratorBuilder.UseLogging(loggerFactory);

            return embeddingGeneratorBuilder.Build(serviceProvider).AsTextEmbeddingGenerationService(serviceProvider);
        });

        return builder;
    }

    public static IKernelBuilder AddTransientGoogleEmbeddingGeneration(
        this IKernelBuilder builder,
        string modelId,
        string apiKey,
        GoogleApiVersion apiVersion,
        string serviceId = null,
        HttpClient httpClient = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(modelId);
        ArgumentException.ThrowIfNullOrEmpty(apiKey);

        builder.Services.AddKeyedTransient<ITextEmbeddingGenerationService>(serviceId, (serviceProvider, _) =>
            new GoogleAITextEmbeddingGenerationService(
                modelId: modelId,
                apiKey: apiKey,
                apiVersion: apiVersion.ToGoogleApiVersion(),
                httpClient: httpClient ?? serviceProvider.GetService<HttpClient>(),
                loggerFactory: serviceProvider.GetService<ILoggerFactory>()));

        return builder;
    }

    public static IKernelBuilder AddTransientGoogleEmbeddingGeneration(
        this IKernelBuilder builder,
        string modelId,
        string apiKey,
        string serviceId = null,
        HttpClient httpClient = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(modelId);
        ArgumentException.ThrowIfNullOrEmpty(apiKey);

        builder.Services.AddKeyedTransient<ITextEmbeddingGenerationService>(serviceId, (serviceProvider, _) =>
            new GoogleAITextEmbeddingGenerationService(
                modelId: modelId,
                apiKey: apiKey,
                httpClient: httpClient ?? serviceProvider.GetService<HttpClient>(),
                loggerFactory: serviceProvider.GetService<ILoggerFactory>()));

        return builder;
    }

    public static IKernelBuilder AddTransientHuggingFaceEmbeddingGeneration(
        this IKernelBuilder builder,
        string model,
        Uri endpoint,
        string apiKey,
        string serviceId = null,
        HttpClient httpClient = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentException.ThrowIfNullOrEmpty(model);
        ArgumentException.ThrowIfNullOrEmpty(apiKey);

        builder.Services.AddKeyedTransient<ITextEmbeddingGenerationService>(serviceId, (serviceProvider, _) =>
            new HuggingFaceTextEmbeddingGenerationService(
                model,
                endpoint,
                apiKey,
                httpClient ?? serviceProvider.GetService<HttpClient>(),
                serviceProvider.GetService<ILoggerFactory>()
            ));

        return builder;
    }

    public static IKernelBuilder AddTransientMistralEmbeddingGeneration(
        this IKernelBuilder builder,
        string model,
        string apiKey,
        Uri endpoint,
        string serviceId = null,
        HttpClient httpClient = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentException.ThrowIfNullOrEmpty(model);
        ArgumentException.ThrowIfNullOrEmpty(apiKey);

        builder.Services.AddKeyedTransient<ITextEmbeddingGenerationService>(serviceId, (serviceProvider, _) =>
            new MistralAITextEmbeddingGenerationService(
                model,
                apiKey,
                endpoint,
                httpClient ?? serviceProvider.GetService<HttpClient>(),
                serviceProvider.GetService<ILoggerFactory>()
            ));

        return builder;
    }

    public static void Configure(this IKernelBuilder kernelBuilder, AiConnectionString connectionString, bool isConnectionTest)
    {
        var connectorType = connectionString.GetActiveProvider();
        ConfigureInternal(kernelBuilder, connectorType, connectionString, isConnectionTest, out _);
    }
    
    public static void Configure(
        this IKernelBuilder kernelBuilder,
        AiIntegrationConfiguration configuration,
        bool isConnectionTest,
        out string resolvedServiceId)
    {
        ConfigureInternal(kernelBuilder, configuration.AiConnectorType ,configuration.Connection, isConnectionTest, out resolvedServiceId);
    }

    private static void ConfigureInternal(this IKernelBuilder kernelBuilder, AiConnectorType connectorType, AiConnectionString connectionString, bool isConnectionTest,
        out string resolvedServiceId)
    {
        var errors = new List<string>();
        if (connectionString.Validate(ref errors) == false)
            throw new InvalidOperationException($"Connection string is invalid due to the following errors:{Environment.NewLine}" +
                                                $" - {string.Join($"{Environment.NewLine} - ", errors)}");
        
        resolvedServiceId = isConnectionTest
            ? AiHelper.ServiceIdentifiers.GenerateTestId()
            : AiHelper.ServiceIdentifiers.Production;
        
        switch (connectorType)
        {
            case AiConnectorType.OpenAi:
                var openAiSettings = connectionString.OpenAiSettings;

                var apiKey = new ApiKeyCredential(openAiSettings.ApiKey);
                var openAiOptions = new OpenAIClientOptions
                {
                    Endpoint = new Uri(openAiSettings.Endpoint),
                    ProjectId = openAiSettings.ProjectId,
                    UserAgentApplicationId = $"RavenDB/{ServerVersion.Version}/{nameof(AiIntegrationTask)}"
                };
                var openAIClient = new OpenAIClient(apiKey, openAiOptions);

                if (isConnectionTest)
                    kernelBuilder.AddTransientOpenAiEmbeddingGeneration(openAiSettings.Model, openAIClient, resolvedServiceId, openAiSettings.Dimensions);
                else
                    kernelBuilder.AddOpenAITextEmbeddingGeneration(openAiSettings.Model, openAIClient, resolvedServiceId, openAiSettings.Dimensions);
                break;

            case AiConnectorType.AzureOpenAi:
                var azureOpenAiSettings = connectionString.AzureOpenAiSettings;

                if (isConnectionTest)
                    kernelBuilder.AddTransientAzureOpenAiEmbeddingGeneration(
                        azureOpenAiSettings.DeploymentName,
                        azureOpenAiSettings.Endpoint,
                        azureOpenAiSettings.ApiKey,
                        resolvedServiceId,
                        azureOpenAiSettings.Model,
                        dimensions: azureOpenAiSettings.Dimensions);
                else
                    kernelBuilder.AddAzureOpenAITextEmbeddingGeneration(
                        azureOpenAiSettings.DeploymentName,
                        azureOpenAiSettings.Endpoint,
                        azureOpenAiSettings.ApiKey,
                        resolvedServiceId,
                        azureOpenAiSettings.Model,
                        dimensions: azureOpenAiSettings.Dimensions);
                break;

            case AiConnectorType.Ollama:
                var ollamaSettings = connectionString.OllamaSettings;
                var ollamaApiConfig = new OllamaApiClient.Configuration { Uri = new Uri(ollamaSettings.Uri), Model = ollamaSettings.Model };

                var ollamaApiClient = new OllamaApiClient(ollamaApiConfig);

                if (isConnectionTest)
                    kernelBuilder.AddTransientOllamaEmbeddingGeneration(ollamaApiClient, resolvedServiceId);
                else
                    kernelBuilder.AddOllamaTextEmbeddingGeneration(ollamaApiClient, resolvedServiceId);
                break;

            case AiConnectorType.Onnx:
                var onnxSettings = connectionString.OnnxSettings;
                if (isConnectionTest)
                    kernelBuilder.AddTransientCustomBertOnnxTextEmbeddingGeneration(onnxSettings.ToBertOnnxOptions(), onnxSettings.Dimensions, resolvedServiceId);
                else
                    kernelBuilder.AddCustomBertOnnxTextEmbeddingGeneration(onnxSettings.ToBertOnnxOptions(), onnxSettings.Dimensions, resolvedServiceId);
                break;

            case AiConnectorType.Google:
                var googleSettings = connectionString.GoogleSettings;

                HttpClient httpClient = null;
                if (googleSettings.Dimensions.HasValue)
                    httpClient = HttpClientExtensions.CreateWithDimensionality(googleSettings.Dimensions.Value);

                if (isConnectionTest)
                {
                    if (googleSettings.AiVersion.HasValue)
                        kernelBuilder.AddTransientGoogleEmbeddingGeneration(
                            googleSettings.Model,
                            googleSettings.ApiKey,
                            googleSettings.AiVersion.Value,
                            resolvedServiceId,
                            httpClient);
                    else
                        kernelBuilder.AddTransientGoogleEmbeddingGeneration(
                            googleSettings.Model,
                            googleSettings.ApiKey,
                            resolvedServiceId,
                            httpClient);
                }
                else
                {
                    if (googleSettings.AiVersion.HasValue)
                        kernelBuilder.AddGoogleAIEmbeddingGeneration(
                            googleSettings.Model,
                            googleSettings.ApiKey,
                            googleSettings.AiVersion.Value.ToGoogleApiVersion(),
                            resolvedServiceId,
                            httpClient);
                    else
                        kernelBuilder.AddGoogleAIEmbeddingGeneration(
                            googleSettings.Model,
                            googleSettings.ApiKey,
                            serviceId: resolvedServiceId,
                            httpClient: httpClient);
                }

                break;

            case AiConnectorType.HuggingFace:
                var huggingFaceSettings = connectionString.HuggingFaceSettings;
                var huggingFaceUri = string.IsNullOrWhiteSpace(huggingFaceSettings.Endpoint) ? null : new Uri(huggingFaceSettings.Endpoint);

                if (isConnectionTest)
                    kernelBuilder.AddTransientHuggingFaceEmbeddingGeneration(
                        huggingFaceSettings.Model,
                        huggingFaceUri,
                        huggingFaceSettings.ApiKey,
                        resolvedServiceId);
                else
                    kernelBuilder.AddHuggingFaceTextEmbeddingGeneration(
                        huggingFaceSettings.Model,
                        huggingFaceUri,
                        huggingFaceSettings.ApiKey,
                        resolvedServiceId);
                break;

            case AiConnectorType.MistralAi:
                var mistralSettings = connectionString.MistralAiSettings;
                var mistralUri = new Uri(mistralSettings.Endpoint);

                if (isConnectionTest)
                    kernelBuilder.AddTransientMistralEmbeddingGeneration(
                        mistralSettings.Model,
                        mistralSettings.ApiKey,
                        mistralUri,
                        resolvedServiceId);
                else
                    kernelBuilder.AddMistralTextEmbeddingGeneration(
                        mistralSettings.Model,
                        mistralSettings.ApiKey,
                        mistralUri,
                        resolvedServiceId);
                break;


            default:
                throw new NotSupportedException($"'{connectorType}' provider is not supported");
        }
        
        if (isConnectionTest)
            kernelBuilder.Services.AddLogging(configure =>
            {
                configure.SetMinimumLevel(LogLevel.Trace);
                configure.AddProvider(new InMemoryLoggerProvider());
            });
    }
}
