using System;
using System.Collections.Generic;
using Microsoft.SemanticKernel.Embeddings;
using Microsoft.SemanticKernel;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.SemanticKernel.Connectors.HuggingFace;
using NuGet.Packaging;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Extensions;

namespace Raven.Server.Documents.AI;

public static class AiHelper
{
    [Experimental("SKEXP0001")]
    public static ITextEmbeddingGenerationService CreateService(AiConnectionString connectionString)
    {
        var kernelBuilder = Kernel.CreateBuilder();
        kernelBuilder.Configure(connectionString, withLogging: false);
        var kernel = kernelBuilder.Build();
        return kernel.GetRequiredService<ITextEmbeddingGenerationService>();
    }

    [Experimental("SKEXP0001")]
    public static (ITextEmbeddingGenerationService, InMemoryLoggerProvider) CreateServicesForTest(EmbeddingsGenerationConfiguration configuration)
    {
        var kernelBuilder = Kernel.CreateBuilder();
        kernelBuilder.Configure(configuration, withLogging: true);
        var kernel = kernelBuilder.Build();

        var embeddingsService = kernel.GetRequiredService<ITextEmbeddingGenerationService>();
        var logger = (InMemoryLoggerProvider)kernel.GetRequiredService<ILoggerProvider>();
        return (embeddingsService, logger);
    }

    /// <summary>
    /// HuggingFaceTextEmbeddingGenerationService has a distinctive feature in its implementation of the ITextEmbeddingGenerationService interface,
    /// which requires processing each element of the array separately instead of batch processing. As a workaround, we created this wrapper method for
    /// the GenerateEmbeddingsAsync() method of the ITextEmbeddingGenerationService interface.
    /// </summary>
    // TODO: Once batch processing is implemented on the SemanticKernel side, or we implement the proper implementation ourselves, we will remove this code.
    [Experimental("SKEXP0001")]
    public static async Task<IList<ReadOnlyMemory<float>>> GenerateEmbeddingsAsync(ITextEmbeddingGenerationService embeddingsGenerationService, IList<string> values)
    {
        IList<ReadOnlyMemory<float>> embeddings;

        if (embeddingsGenerationService is HuggingFaceTextEmbeddingGenerationService)
        {
            embeddings = new List<ReadOnlyMemory<float>>();
            string[] singleItemArray = new string[1];

            foreach (string value in values)
            {
                singleItemArray[0] = value;
                embeddings.AddRange(await embeddingsGenerationService.GenerateEmbeddingsAsync(singleItemArray));
            }
        }
        else
        {
            embeddings = await embeddingsGenerationService.GenerateEmbeddingsAsync(values);
        }

        return embeddings;
    }

    [Experimental("SKEXP0001")]
    public static ReadOnlyMemory<float> GenerateEmbedding(ITextEmbeddingGenerationService embeddingGenerationService, string value) =>
        embeddingGenerationService.GenerateEmbeddingsAsync([value]).GetAwaiter().GetResult()[0];
}
