using Microsoft.SemanticKernel.Embeddings;
using Microsoft.SemanticKernel;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;
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
}
