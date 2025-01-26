using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.Connectors.Onnx;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Server.Documents.Indexes.VectorSearch;

namespace Raven.Server.Documents.ETL.Providers.AI.Extensions;

public static class KernelBuilderExtensions
{
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
