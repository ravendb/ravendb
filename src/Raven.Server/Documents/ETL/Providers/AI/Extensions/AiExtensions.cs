using System;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.Connectors.Google;
using Microsoft.SemanticKernel.Connectors.HuggingFace;
using Microsoft.SemanticKernel.Connectors.Onnx;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Server.Documents.Indexes.VectorSearch;
using GoogleAIVersion = Raven.Client.Documents.Operations.ETL.AI.GoogleAIVersion;

#pragma warning disable SKEXP0070

namespace Raven.Server.Documents.ETL.Providers.AI.Extensions;

public static class AiExtensions
{
    public static BertOnnxOptions ToBertOnnxOptions(this OnnxSettings settings)
    {
        var defaults = new BertOnnxOptions();

        return new BertOnnxOptions
        {
            CaseSensitive = settings.CaseSensitive ?? defaults.CaseSensitive,
            MaximumTokens = settings.MaximumTokens ?? defaults.MaximumTokens,
            ClsToken = settings.ClsToken ?? defaults.ClsToken,
            UnknownToken = settings.UnknownToken ?? defaults.UnknownToken,
            SepToken = settings.SepToken ?? defaults.UnknownToken,
            PadToken = settings.PadToken ?? defaults.PadToken,
            UnicodeNormalization = settings.UnicodeNormalization ?? defaults.UnicodeNormalization,
            PoolingMode = settings.PoolingMode?.ToEmbeddingPoolingMode() ?? defaults.PoolingMode,
            NormalizeEmbeddings = settings.NormalizeEmbeddings ?? defaults.NormalizeEmbeddings
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

    [Experimental("SKEXP0070")]
    public static IKernelBuilder AddHuggingFaceTextEmbeddingGeneration2(
        this IKernelBuilder builder,
        HuggingFaceSettings settings,
        string serviceId = null)
    {
        var endpoint = string.IsNullOrWhiteSpace(settings.Endpoint) ? null : new Uri(settings.Endpoint);

        builder.Services.AddKeyedSingleton<ITextEmbeddingGenerationService>(serviceId,
            new HuggingFaceTextEmbeddingGenerationService(
                settings.Model,
                endpoint,
                settings.ApiKey
            ));

        return builder;
    }
}
