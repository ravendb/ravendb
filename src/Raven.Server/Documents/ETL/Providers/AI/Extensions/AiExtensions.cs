using System;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.Connectors.Onnx;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Server.Documents.Indexes.VectorSearch;

#pragma warning disable SKEXP0070

namespace Raven.Server.Documents.ETL.Providers.AI.Extensions;

public static class AiExtensions
{
    public static BertOnnxOptions ToBertOnnxOptions(this OnnxSettings settings)
    {
        return new()
        {
            CaseSensitive = settings.CaseSensitive,
            MaximumTokens = settings.MaximumTokens,
            ClsToken = settings.ClsToken,
            UnknownToken = settings.UnknownToken,
            SepToken = settings.SepToken,
            PadToken = settings.PadToken,
            UnicodeNormalization = settings.UnicodeNormalization,
            PoolingMode = settings.PoolingMode.ToEmbeddingPoolingMode(),
            NormalizeEmbeddings = settings.NormalizeEmbeddings
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
