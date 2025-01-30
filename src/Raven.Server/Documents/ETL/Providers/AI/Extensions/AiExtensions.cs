using System;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.Connectors.Onnx;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Server.Documents.Indexes.VectorSearch;
using GoogleAIVersion = Raven.Client.Documents.Operations.ETL.AI.GoogleAIVersion;

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
}
