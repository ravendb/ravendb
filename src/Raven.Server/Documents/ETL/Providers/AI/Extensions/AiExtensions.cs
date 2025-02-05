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
using Microsoft.SemanticKernel.Connectors.Onnx;
using Microsoft.SemanticKernel.Connectors.OpenAI;
using Microsoft.SemanticKernel.Embeddings;
using OllamaSharp;
using OpenAI;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.ServerWide;
using GoogleAIVersion = Raven.Client.Documents.Operations.ETL.AI.GoogleAIVersion;
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

    public static Microsoft.SemanticKernel.Connectors.Google.GoogleAIVersion ToGoogleAiVersion(this GoogleAIVersion googleAiVersion)
    {
        switch (googleAiVersion)
        {
            case GoogleAIVersion.V1:
                return Microsoft.SemanticKernel.Connectors.Google.GoogleAIVersion.V1;
            case GoogleAIVersion.V1_Beta:
                return Microsoft.SemanticKernel.Connectors.Google.GoogleAIVersion.V1_Beta;
            default:
                throw new ArgumentOutOfRangeException(nameof(googleAiVersion), googleAiVersion, null);
        }
    }

    [Experimental("SKEXP0070")]
    public static IKernelBuilder AddCustomBertOnnxTextEmbeddingGeneration(
        this IKernelBuilder builder,
        BertOnnxOptions options = null,
        string serviceId = null)
    {
        builder.Services.AddKeyedSingleton<ITextEmbeddingGenerationService>(
            serviceId,
            GenerateEmbeddings.CreateTextEmbeddingGenerationService(options));

        return builder;
    }

    public static IKernelBuilder AddTransientCustomBertOnnxTextEmbeddingGeneration(
        this IKernelBuilder builder,
        BertOnnxOptions options = null,
        string serviceId = null)
    {
        builder.Services.AddKeyedTransient<ITextEmbeddingGenerationService>(
            serviceId,
            (_, _) => GenerateEmbeddings.CreateTextEmbeddingGenerationService(options));

        return builder;
    }

    public static IKernelBuilder AddTransientOpenAiEmbeddingGeneration(
        this IKernelBuilder builder,
        string modelId,
        OpenAIClient openAIClient = null,
        string serviceId = null,
        int? dimensions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(modelId);

        builder.Services.AddKeyedTransient<ITextEmbeddingGenerationService>(
            serviceId,
            (serviceProvider, _) => new OpenAITextEmbeddingGenerationService(
                modelId,
                openAIClient ?? serviceProvider.GetRequiredService<OpenAIClient>(),
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
        GoogleAIVersion apiVersion,
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
                apiVersion: apiVersion.ToGoogleAiVersion(),
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
        Uri endpoint = null,
        string apiKey = null,
        string serviceId = null,
        HttpClient httpClient = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

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

    [Experimental("SKEXP0001")]
    public static void Configure(
        this IKernelBuilder kernelBuilder,
        AiEtlConfiguration configuration,
        bool isConnectionTest,
        out string resolvedServiceId)
    {
        resolvedServiceId = isConnectionTest
            ? AiHelper.ServiceIdentifiers.GenerateTestId()
            : AiHelper.ServiceIdentifiers.Production;

        var errors = new List<string>();
        if (configuration.Connection.Validate(ref errors) == false)
            throw new InvalidOperationException($"Connection string is invalid due to the following errors:{Environment.NewLine}" +
                                                $" - {string.Join($"{Environment.NewLine} - ", errors)}");

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

                if (isConnectionTest)
                    kernelBuilder.AddTransientOpenAiEmbeddingGeneration(openAiSettings.Model, openAIClient, resolvedServiceId);
                else
                    kernelBuilder.AddOpenAITextEmbeddingGeneration(openAiSettings.Model, openAIClient, resolvedServiceId);
                break;

            case AiConnectorType.AzureOpenAI:
                var azureOpenAiSettings = configuration.Connection.AzureOpenAiSettings;

                if (isConnectionTest)
                    kernelBuilder.AddTransientAzureOpenAiEmbeddingGeneration(
                        azureOpenAiSettings.DeploymentName,
                        azureOpenAiSettings.Endpoint,
                        azureOpenAiSettings.ApiKey,
                        resolvedServiceId,
                        azureOpenAiSettings.Model);
                else
                    kernelBuilder.AddAzureOpenAITextEmbeddingGeneration(
                        azureOpenAiSettings.DeploymentName,
                        azureOpenAiSettings.Endpoint,
                        azureOpenAiSettings.ApiKey,
                        resolvedServiceId,
                        azureOpenAiSettings.Model);
                break;

            case AiConnectorType.Ollama:
                var ollamaSettings = configuration.Connection.OllamaSettings;
                var ollamaApiConfig = new OllamaApiClient.Configuration { Uri = new Uri(ollamaSettings.Uri), Model = ollamaSettings.Model };

                var ollamaApiClient = new OllamaApiClient(ollamaApiConfig);

                if (isConnectionTest)
                    kernelBuilder.AddTransientOllamaEmbeddingGeneration(ollamaApiClient, resolvedServiceId);
                else
                    kernelBuilder.AddOllamaTextEmbeddingGeneration(ollamaApiClient, resolvedServiceId);
                break;

            case AiConnectorType.Onnx:
                var onnxSettings = configuration.Connection.OnnxSettings;
                if (isConnectionTest)
                    kernelBuilder.AddTransientCustomBertOnnxTextEmbeddingGeneration(onnxSettings.ToBertOnnxOptions(), resolvedServiceId);
                else
                    kernelBuilder.AddCustomBertOnnxTextEmbeddingGeneration(onnxSettings.ToBertOnnxOptions(), resolvedServiceId);
                break;

            case AiConnectorType.Google:
                var googleSettings = configuration.Connection.GoogleSettings;

                if (isConnectionTest)
                {
                    if (googleSettings.AiVersion.HasValue)
                        kernelBuilder.AddTransientGoogleEmbeddingGeneration(
                            googleSettings.Model,
                            googleSettings.ApiKey,
                            googleSettings.AiVersion.Value,
                            resolvedServiceId);
                    else
                        kernelBuilder.AddTransientGoogleEmbeddingGeneration(
                            googleSettings.Model,
                            googleSettings.ApiKey,
                            serviceId: resolvedServiceId);
                }
                else
                {
                    if (googleSettings.AiVersion.HasValue)
                        kernelBuilder.AddGoogleAIEmbeddingGeneration(
                            googleSettings.Model,
                            googleSettings.ApiKey,
                            googleSettings.AiVersion.Value.ToGoogleAiVersion(),
                            resolvedServiceId);
                    else
                        kernelBuilder.AddGoogleAIEmbeddingGeneration(
                            googleSettings.Model,
                            googleSettings.ApiKey,
                            serviceId: resolvedServiceId);
                }

                break;

            case AiConnectorType.HuggingFace:
                var huggingFaceSettings = configuration.Connection.HuggingFaceSettings;
                var uri = string.IsNullOrWhiteSpace(huggingFaceSettings.Endpoint) ? null : new Uri(huggingFaceSettings.Endpoint);

                if (isConnectionTest)
                    kernelBuilder.AddTransientHuggingFaceEmbeddingGeneration(
                        huggingFaceSettings.Model,
                        uri,
                        huggingFaceSettings.ApiKey,
                        resolvedServiceId);
                else
                    kernelBuilder.AddHuggingFaceTextEmbeddingGeneration(
                        huggingFaceSettings.Model,
                        uri,
                        huggingFaceSettings.ApiKey,
                        resolvedServiceId);
                break;

            default:
                throw new NotSupportedException($"'{configuration.AiConnectorType}' provider is not supported");
        }

        if (isConnectionTest)
            kernelBuilder.Services.AddLogging(configure =>
            {
                configure.SetMinimumLevel(LogLevel.Trace);
                configure.AddProvider(new InMemoryLoggerProvider());
            });
    }
}
